import os.path
import paramiko
import traceback

from common import *

SFTP_Client = None
SYNC_Root = ["config", "run", "strategy", "utils", "data", 'trade', "warm"]
DIR_Except = {"__pycache__"}
FILE_Except = {"analyze.py", 'deployer.py'}


def _check_local_file():
    if not os.path.exists(localbase):
        print(localbase + ' ERROR.')
        return False

    for item in SYNC_Root:
        if not os.path.exists(os.path.join(localbase, item)):
            print(item + " Error")
            return False
    return True


def recur_put(lb, rb, cur_df):
    remote_base = rb + "/" + cur_df
    local_base = os.path.join(lb, cur_df)
    SFTP_Client.mkdir(remote_base)
    for item in os.listdir(local_base):
        if (item in DIR_Except) or (item in FILE_Except):
            print("Item " + str(item) + " is Skipped.")
            continue
        absp = os.path.join(local_base, item)
        if os.path.isfile(absp):
            SFTP_Client.put(absp, remote_base + "/" + item)
            print(str(absp) + " is Synced to " + remote_base)
        elif os.path.isdir(absp):
            recur_put(local_base, remote_base, item)
        else:
            print(str(absp) + " is nothing")


if __name__ == '__main__':
    ssh = None
    try:
        # 确认本地目录及文件正常
        localbase = conf_dict['SFTP']['LocalBase']
        remotebase = conf_dict['SFTP']['RemoteBase']
        if not _check_local_file():
            raise FileExistsError
        # 建立ssh连接
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=conf_dict['SFTP']['Host'], port=conf_dict['SFTP']['Port'],
                    username=conf_dict['SFTP']['User'], password=conf_dict['SFTP']['Passwd'])
        # 清除远端待同步目录
        for d in SYNC_Root:
            abspath = remotebase + "/" + d
            command = 'rm -rf ' + abspath
            ssh.exec_command(command)
        # 递归同步本地文件
        SFTP_Client = paramiko.SFTPClient.from_transport(ssh.get_transport())
        for d in SYNC_Root:
            recur_put(localbase, remotebase, d)
    except FileExistsError:
        traceback.print_exc()
    except Exception:
        traceback.print_exc()
    finally:
        ssh.close()
