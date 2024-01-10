import matplotlib.pyplot as plt
import numpy as np
import pickle
import os
import datetime
from tqdm import tqdm

attempt_num = 20

pb = tqdm(total=attempt_num, desc='')

for i in range(20):
    os.chdir('tmp')

    for file in os.listdir():
        os.remove(file)

    pb.set_description('Generating')
    os.system('..\\generate.exe')
    pb.set_description('Simulating')
    os.system('..\\simulate.exe')

    pb.set_description('Reading data')
    sc_bc_act = np.array(pickle.load(open('sc_broadcast_activity.pkl', 'rb')))
    sc_dp_act = np.array(pickle.load(open('sc_dispatch_activity.pkl', 'rb')))
    sc_di_act = np.array(pickle.load(open('sc_discard_activity.pkl', 'rb')))
    sc_lat = np.array(pickle.load(open('sc_latency.pkl', 'rb')))
    sc_cong = np.array(pickle.load(open('sc_congestion.pkl', 'rb')))

    pb.set_description('Plotting')
    fig, ax = plt.subplots()
    ax.hist(sc_bc_act, bins=400, density=True, alpha=0.5, label='broadcast activity')
    ax.hist(sc_dp_act, bins=400, density=True, alpha=0.5, label='dispatch activity')
    ax.hist(sc_di_act, bins=400, density=True, alpha=0.5, label='discard activity')
    ax.legend(loc='upper right')
    ax.set_xlabel('activities density histogram')
    fig.savefig('activity.png', dpi=600)

    fig, ax = plt.subplots()
    ax.scatter(sc_lat[:, 0], sc_lat[:, 1], s=0.1)
    ax.set_xlabel('time')
    ax.set_ylabel('latency')
    fig.savefig('latency.png', dpi=600)

    fig, ax = plt.subplots(dpi=150)
    ax.scatter(sc_cong[:, 0], sc_cong[:, 1], s=0.1)
    ax.set_xlabel('time')
    ax.set_ylabel('congestion')
    fig.savefig('congestion.png', dpi=600)

    plt.close('all')

    pb.set_description('Saving')
    time_str = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    out_subdir_name = '{}#{}'.format(time_str, i)
    os.chdir('..')
    os.mkdir('out\\' + out_subdir_name)

    os.rename('tmp\\activity.png', 'out\\' + out_subdir_name + '\\activity.png')
    os.rename('tmp\\latency.png', 'out\\' + out_subdir_name + '\\latency.png')
    os.rename('tmp\\congestion.png', 'out\\' + out_subdir_name + '\\congestion.png')

    pb.update(1)