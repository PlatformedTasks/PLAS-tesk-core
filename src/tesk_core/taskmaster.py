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

created_jobs = []
created_platform = []
helm_values = list()
poll_interval = 5
task_volume_basename = 'task-volume'
args = None
logger = None


def run_executor(executor, namespace, pvc=None):
    # remove 'transfer-volume' volumeMount only for the executor
    result = next((volumemount for volumemount in pvc.volume_mounts if volumemount['name'] == 'transfer-volume'), None)
    if result:
        pvc.volume_mounts.remove(result)

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

        exit_cancelled('Got status ' + status)


# TODO move this code to PVC class

def append_mount(volume_mounts, name, path, pvc, transfer=False):
    # Checks all mount paths in volume_mounts if the path given is already in
    # there
    duplicate = next(
        (mount for mount in volume_mounts if mount['mountPath'] == path),
        None)
    # If not, add mount path
    if duplicate is None and not transfer:
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
        if 'tmconfig' in aninput.keys() and aninput['tmconfig']:
            dirnm = aninput['path'].split('/')[-2]
            append_mount(volume_mounts, 'transfer-volume', dirnm, pvc, transfer=True)
        else:
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

        for input in data['inputs']:
            if 'tmconfig' in input.keys() and input['tmconfig']:
                helm_values.append(input['path'])

    for executor in data['executors']:
        if executor['kind'] == "Job":
            run_executor(executor, args.namespace, pvc)
        elif executor['kind'] == "helm":
            run_chart(executor, args.namespace, helm_values, pvc)
            # WAIT UNTIL PLATFORM DEPLOYED THEN RUN JOB
            mounts = executor['job']['spec']['template']['spec']['containers'][0].setdefault('volumeMounts', [])
            mounts.extend([{"name": "executor-volume", "mountPath": "/tmp/generated"}])
            volumes = executor['job']['spec']['template']['spec'].setdefault('volumes', [])
            volumes.extend([{"name": "executor-volume", "configMap": {"name": f"{task_name}-platform-{data['executors'][0]['chart_name']}-cm", 
                                                                        "defaultMode": 420,
                                                                        "items": [
                                                                            {"key": "executor.config", "mode": 438, "path": "executor.config"},
                                                                            {"key": "executor.init", "mode": 438, "path": "executor.init"}]}}])
            print("Added custom configMap for the executor.")
            logging.debug("Added custom configMap for the executor.")

            run_executor(executor["job"], args.namespace, pvc)

    # run executors
    logging.debug("Finished running executors")

    if created_platform:
        for platform in created_platform:
            helm_client.helm_uninstall(platform)

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
            exit_cancelled('Got status ' + status)
        else:
            pvc.delete()


def run_chart(executor, namespace, helm_values, pvc=None):
    release_name = f"{executor['job']['metadata']['labels']['taskmaster-name']}-platform"
    chart_name = executor["chart_name"]
    chart_repo = executor["chart_repo"]
    chart_version = executor["chart_version"]

    helm_client.helm_add_repo(chart_repo)

    installed_platform = helm_client.helm_install(release_name=release_name, chart_name=chart_name,
                                                  chart_version=chart_version, chart_values=helm_values, namespace=namespace)

    if installed_platform and installed_platform.returncode == 0:
        created_platform.append(release_name)
    else:
        exit_cancelled("Error installing helm.")


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

    for platform in created_platform:
        helm_client.helm_uninstall(platform)

    for job in created_jobs:
        job.delete()


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
