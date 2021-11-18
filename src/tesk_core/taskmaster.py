#!/usr/bin/env python3

import argparse
import json
import yaml
import logging
import os
import re
import sys
import glob
import time

from kubernetes import config

from tesk_core.job import Job
from tesk_core.pvc import PVC
from tesk_core.filer_class import Filer

# import src.tesk_core.helm_client as helm_client
import tesk_core.helm_client as helm_client
# import helm_client
# sys.argv.append("-n")
# sys.argv.append("default")
# sys.argv.append("-fn")
# sys.argv.append("eu.gcr.io/tes-wes/filer")
# sys.argv.append("-fv")
# sys.argv.append("v0.10.0")
# sys.argv.append("--localKubeConfig")
#
# #Environments taken from YAML
# os.environ["TESK_FTP_USERNAME"] = "uftp"
# os.environ["TESK_FTP_PASSWORD"] = "uftp"
# os.environ["CONTAINER_BASE_PATH"] = "/transfer"
# os.environ["HOST_BASE_PATH"] = "/tmp"
# os.environ["TRANSFER_PVC_NAME"] = "transfer-pvc"
# os.environ["FILER_BACKOFF_LIMIT"] = "1"
# os.environ["EXECUTOR_BACKOFF_LIMIT"] = "1"
# os.environ["DEPLOY_WITH_HELM"] = "1"

created_jobs = []
created_platform = []
poll_interval = 5
task_volume_basename = 'task-volume'
args = None
logger = None

def run_executor(executor, namespace, pvc=None):
    jobname = executor['metadata']['name']
    spec = executor['spec']['template']['spec']

    if os.environ.get('EXECUTOR_BACKOFF_LIMIT') is not None:
        executor['spec'].update({'backoffLimit': int(os.environ['EXECUTOR_BACKOFF_LIMIT'])})

    if pvc is not None:
        mounts = spec['containers'][0].setdefault('volumeMounts', [])
        mounts.extend(pvc.volume_mounts)
        volumes = spec.setdefault('volumes', [])
        volumes.extend([{'name': task_volume_basename, 'persistentVolumeClaim': {
            'readonly': False, 'claimName': pvc.name}}])

    logger.debug('Created job: ' + jobname)
    job = Job(executor, jobname, namespace)
    logger.debug('Job spec: ' + str(job.body))

    global created_jobs
    created_jobs.append(job.body)

    status = job.run_to_completion(poll_interval, check_cancelled,args.pod_timeout)
    if status != 'Complete':
        if status == 'Error':
            job.delete()

        # print("PRIMA DI ELIMINARE L'EX ASPETTA 300 sec")
        # time.sleep(300)
        # print("AAA 79 #########")
        exit_cancelled('Got status ' + status)

# TODO move this code to PVC class

def append_mount(volume_mounts, name, path, pvc):
    # Checks all mount paths in volume_mounts if the path given is already in
    # there
    duplicate = next(
        (mount for mount in volume_mounts if mount['mountPath'] == path),
        None)
    # If not, add mount path
    if duplicate is None:
        subpath = pvc.get_subpath()
        logger.debug(' '.join(
            ['appending' + name +
             'at path' + path +
             'with subPath:' + subpath]))
        volume_mounts.append(
            {'name': name, 'mountPath': path, 'subPath': subpath})


def dirname(iodata):
    if iodata['type'] == 'FILE':
        # strip filename from path
        r = '(.*)/'
        dirname = re.match(r, iodata['path']).group(1)
        logger.debug('dirname of ' + iodata['path'] + 'is: ' + dirname)
    elif iodata['type'] == 'DIRECTORY':
        dirname = iodata['path']

    return dirname


def generate_mounts(data, pvc):
    volume_mounts = []

    # gather volumes that need to be mounted, without duplicates
    volume_name = task_volume_basename
    for volume in data['volumes']:
        append_mount(volume_mounts, volume_name, volume, pvc)

    # gather other paths that need to be mounted from inputs/outputs FILE and
    # DIRECTORY entries
    for aninput in data['inputs']:
        dirnm = dirname(aninput)
        append_mount(volume_mounts, volume_name, dirnm, pvc)

    for anoutput in data['outputs']:
        dirnm = dirname(anoutput)
        append_mount(volume_mounts, volume_name, dirnm, pvc)

    return volume_mounts


def init_pvc(data, filer):
    if data['executors'][0]['kind'] == "Job":
        task_name = data['executors'][0]['metadata']['labels']['taskmaster-name']
    elif data['executors'][0]['kind'] == "helm":
        task_name = data['executors'][0]["job"]['metadata']['labels']['taskmaster-name']
    else:
        exit_cancelled("No task defined.")

    pvc_name = task_name + '-pvc'
    pvc_size = data['resources']['disk_gb']
    pvc = PVC(pvc_name, pvc_size, args.namespace)

    mounts = generate_mounts(data, pvc)
    logging.debug(mounts)
    logging.debug(type(mounts))
    pvc.set_volume_mounts(mounts)
    filer.add_volume_mount(pvc)

    pvc.create()
    # to global var for cleanup purposes
    global created_pvc
    created_pvc = pvc

    if os.environ.get('NETRC_SECRET_NAME') is not None:
        filer.add_netrc_mount(os.environ.get('NETRC_SECRET_NAME'))

    filerjob = Job(
        filer.get_spec('inputs', args.debug),
        task_name + '-inputs-filer',
        args.namespace)

    global created_jobs
    created_jobs.append(filerjob)
    # filerjob.run_to_completion(poll_interval)
    status = filerjob.run_to_completion(poll_interval, check_cancelled, args.pod_timeout)
    if status != 'Complete':
        # print("AAA 200 #########")
        exit_cancelled('Got status ' + status)

    return pvc


def run_task(data, filer_name, filer_version):
    if data['executors'][0]['kind'] == "Job":
        task_name = data['executors'][0]['metadata']['labels']['taskmaster-name']
    elif data['executors'][0]['kind'] == "helm":
        task_name = data['executors'][0]['job']['metadata']['labels']['taskmaster-name']
    else:
        exit_cancelled("No task defined.")

    pvc = None

    if data['volumes'] or data['inputs'] or data['outputs']:
        filer = Filer(task_name + '-filer', data, filer_name, filer_version, args.pull_policy_always)

        if os.environ.get('TESK_FTP_USERNAME') is not None:
            filer.set_ftp(
                os.environ['TESK_FTP_USERNAME'],
                os.environ['TESK_FTP_PASSWORD'])

        if os.environ.get('FILER_BACKOFF_LIMIT') is not None:
            filer.set_backoffLimit(int(os.environ['FILER_BACKOFF_LIMIT']))

        pvc = init_pvc(data, filer)

    for executor in data['executors']:
        if executor['kind'] == "Job":
            run_executor(executor, args.namespace, pvc)
        elif executor['kind'] == "helm":
            run_chart(executor, args.namespace, pvc)
            # WAIT UNTIL PLATFORM DEPLOYED THEN RUN JOB
            print("ADDING EXECUTOR CONFIGMAP")
            mounts = executor['job']['spec']['template']['spec']['containers'][0].setdefault('volumeMounts', [])
            mounts.extend([{"name": "executor-volume", "mountPath": "/tmp/generated"}])
            volumes = executor['job']['spec']['template']['spec'].setdefault('volumes', [])
            volumes.extend([{"name": "executor-volume", "configMap": {"defaultMode": 420, "items": [
                {"key": "hostfile.config", "mode": 438, "path": "hostfile"}], "name": "executor-volume-cm"}}])

            run_executor(executor["job"], args.namespace, pvc)

    # run executors
    logging.debug("Finished running executors")

    # upload files and delete pvc
    if data['volumes'] or data['inputs'] or data['outputs']:
        filerjob = Job(
            filer.get_spec('outputs', args.debug),
            task_name + '-outputs-filer',
            args.namespace)

        global created_jobs
        created_jobs.append(filerjob)

        # filerjob.run_to_completion(poll_interval)
        status = filerjob.run_to_completion(poll_interval, check_cancelled, args.pod_timeout)
        if status != 'Complete':
            # print("AAA 254 #########")
            exit_cancelled('Got status ' + status)
        else:
            pvc.delete()


def run_chart(executor, namespace, pvc=None):
    release_name = f"{executor['job']['metadata']['labels']['taskmaster-name']}-platform"
    chart_name = executor["chart_name"]
    chart_repo = executor["chart_repo"]
    chart_version = executor["chart_version"]

    helm_client.helm_add_repo(chart_repo)
    installed_platfrom = helm_client.helm_install(release_name=release_name, chart_name=chart_name, chart_version=chart_version,
                             namespace=namespace)
    if installed_platfrom.returncode == 0:
        created_platform.append(release_name)

    # if not chart_repo:
    #     print("TRY LOCAL HELM")
    #     try:
    #         with open(f"{chart_repo}/values.yaml") as f:
    #             values_dict = yaml.safe_load(f)
    #     except Exception as err:
    #         print("Error opening Helm values file:", err)
    #         sys.exit(0)
    #
    #     builder = ChartBuilder(ChartInfo(api_version="3.2.4", name=chart_name, version="1", app_version=chart_version, dependencies=[
    #                 ChartDependency(name=chart_repo.split("/")[-1], version="1", repository=f"file:///{chart_repo}",
    #                                 local_repo_name="local-repo", is_local=True), ], ), [],
    #                            values=Values(values_dict), namespace=namespace)
    #     # USE upgrade_chart instead of install_chart
    #     # builder.upgrade_chart()
    #     builder.install_chart({"dependency-update": None, "wait": None})
    # else:
    # print("TRY REPO HELM")
    # helm_client.helm_add_repo(chart_repo)
    # helm_client.helm_install(release_name=f"{task_name}-platform", chart_name=chart_name, chart_version=chart_version, namespace=namespace)

    # builder = ChartBuilder(ChartInfo(api_version="3.2.4",
    #                                  name=f"{task_name}-platform",
    #                                  version="0.1.0",
    #                                  app_version="0.15.0",
    #                                  dependencies=[ChartDependency(
    #                                      name=chart_name,
    #                                      version=chart_version,
    #                                      repository=chart_repo,
    #                                      local_repo_name=f"taskmaster-repo", ), ],
    #                                  ),
    #                        [],
    #                        namespace=namespace)
    #
    # builder.install_chart({"dependency-update": None, "wait": None})


def newParser():

    parser = argparse.ArgumentParser(description='TaskMaster main module')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        'json',
        help='string containing json TES request, required if -f is not given',
        nargs='?')
    group.add_argument(
        '-f',
        '--file',
        help='TES request as a file or \'-\' for stdin, required if json is not given')

    parser.add_argument(
        '-p',
        '--poll-interval',
        help='Job polling interval',
        default=5)
    parser.add_argument(
        '-pt',
        '--pod-timeout',
        type=int,
        help='Pod creation timeout',
        default=240)
    parser.add_argument(
        '-fn',
        '--filer-name',
        help='Filer image version',
        default='eu.gcr.io/tes-wes/filer')
    parser.add_argument(
        '-fv',
        '--filer-version',
        help='Filer image version',
        default='v0.1.9')
    parser.add_argument(
        '-n',
        '--namespace',
        help='Kubernetes namespace to run in',
        default='default')
    parser.add_argument(
        '-s',
        '--state-file',
        help='State file for state.py script',
        default='/tmp/.teskstate')
    parser.add_argument(
        '-d',
        '--debug',
        help='Set debug mode',
        action='store_true')
    parser.add_argument(
        '--localKubeConfig',
        help='Read k8s configuration from localhost',
        action='store_true')
    parser.add_argument(
        '--pull-policy-always',
        help="set imagePullPolicy = 'Always'",
        action='store_true')


    return parser


def newLogger(loglevel):
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S',
        level=loglevel)
    logging.getLogger('kubernetes.client').setLevel(logging.CRITICAL)
    logger = logging.getLogger(__name__)

    return logger


def main(argv=None):

    parser = newParser()
    global args

    args = parser.parse_args()

    poll_interval = args.poll_interval

    loglevel = logging.ERROR
    if args.debug:
        loglevel = logging.DEBUG

    global logger
    logger = newLogger(loglevel)
    logger.debug('Starting taskmaster')

    # Get input JSON
    if args.file is None:
        data = json.loads(args.json)
    elif args.file == '-':
        data = json.load(sys.stdin)
    else:
        with open(args.file) as fh:
            data = json.load(fh)

    # Load kubernetes config file
    if args.localKubeConfig:
        config.load_kube_config()
    else:
        config.load_incluster_config()

    global created_pvc
    created_pvc = None

    # Check if we're cancelled during init
    if check_cancelled():
        exit_cancelled('Cancelled during init')

    run_task(data, args.filer_name, args.filer_version)


def clean_on_interrupt():
    logger.debug('Caught interrupt signal, deleting jobs and pvc')

    for job in created_jobs:
        job.delete()

    for platform in created_platform:
        helm_client.helm_uninstall(platform)



def exit_cancelled(reason='Unknown reason'):
    logger.error('Cancelling taskmaster: ' + reason)
    sys.exit(0)


def check_cancelled():

    labelInfoFile = '/podinfo/labels'

    if not os.path.exists(labelInfoFile):
        return False

    with open(labelInfoFile) as fh:
        for line in fh.readlines():
            name, label = line.split('=')
            logging.debug('Got label: ' + label)
            if label == '"Cancelled"':
                return True

    return False


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        clean_on_interrupt()
