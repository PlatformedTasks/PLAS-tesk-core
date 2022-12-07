import subprocess
from tesk_core import path

repo_name = "taskmaster-repo"

def helm_add_repo(repo_url):
    try:
        print(f"Adding '{repo_url}' as '{repo_name}'")
        repo_add = subprocess.run(['helm', 'repo', 'add', repo_name, repo_url, '--force-update'], capture_output=True, text=True, check=True)
        print(repo_add.stdout)
        print(f"Updating helm repositories...")
        repo_update = subprocess.run(['helm', 'repo', 'update'], capture_output=True, text=True, check=True)
        print(repo_update.stdout)
    except subprocess.CalledProcessError as err:
        print(err.stderr)
    except Exception as err:
        print(err)


def pvc_to_helm(pvc):
    helm_set = list()

    for position, volume_mount in enumerate(pvc.volume_mounts):
        for volume_mount_key in volume_mount:
            helm_set.append(
                f"--set volumeMounts[{position}].{volume_mount_key}={pvc.volume_mounts[volume_mount_key]}")

    helm_set.append(
        f"--set volumes[0].name={pvc.volume_mounts[0]['name']} --set volumes[0].persistentVolumeClaim.claimName={pvc.spec['metadata']['name']}")

    setHelmVolumes = ' '.join(helm_set)
    return setHelmVolumes


def helm_install(release_name, chart_name, chart_version, chart_values, task_name, namespace="default", pvc=None):
    try:
        chart = f"{repo_name}/{chart_name}"
        print(f"Installing '{release_name}' from '{chart}' in namespace '{namespace}'...")

        helm_command = [
            'helm', 
            'install', 
            release_name, 
            chart, 
            f'--namespace={namespace}', 
            '--wait']

        if chart_version:
            helm_command.append(f'--version={chart_version}')
        if chart_values:
            for chart_file in chart_values:
                helm_command.append(f'-f={str(chart_file)}')
        if pvc:
            helm_command.append(pvc_to_helm(pvc))

        release_install = subprocess.run(helm_command, capture_output=True, text=True, check=True)
        print(release_install.stdout)
        
        return release_install

    except subprocess.CalledProcessError as err:
        print(err.stderr)

    except Exception as err:
        print(err)


def helm_uninstall(release_name, namespace="default"):
    try:
        print(f"Uninstalling '{release_name}'...")
        release_uninstall = subprocess.run(['helm', 'uninstall', release_name, f'--namespace={namespace}'], capture_output=True, text=True, check=True)
        print(release_uninstall.stdout)

    except subprocess.CalledProcessError as err:
        print(err.stderr)

    except Exception as err:
        print(err)
