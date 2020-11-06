import os


def write_throughput_all_experiments(all_experiments_metrics):
    with open(f'throughput.csv', 'w') as f:
        for exp_num, exp_metrics in all_experiments_metrics.items():
            throughput_aggregates = [exp_metrics['min'][2],
                                     exp_metrics['avg'][2], exp_metrics['max'][2]]
            result = [str(exp_num)]
            result.extend(["{:.2f}".format(x) for x in throughput_aggregates])
            f.write(','.join(result))
            f.write('\n')


def write_all_metrics_all_experiments(all_experiments_metrics):
    with open(f'all_metrics.csv', 'w') as f:
        for exp_num, exp_metrics in all_experiments_metrics.items():
            result = [str(exp_num)]
            for i in range(len(exp_metrics['min'])):
                aggregates = [exp_metrics['min'][i],
                              exp_metrics['avg'][i], exp_metrics['max'][i]]
                result.extend(["{:.2f}".format(x) for x in aggregates])
            f.write(','.join(result))
            f.write('\n')


def write_aggregate_metrics(experiment_folders):
    # Initialize aggregate metrics
    aggregate_metrics = {
        'min': [],
        'avg': [],
        'max': []
    }

    all_experiments_metrics = {}

    for index, folder in enumerate(experiment_folders, 5):
        # Initialize aggregate metrics
        aggregate_metrics = {
            'min': [],
            'avg': [],
            'max': []
        }
        sum_metrics = []
        count = 0

        # Compute aggregates
        for file in os.listdir(folder):
            if file.endswith('.metrics'):
                count += 1
                with open(f'{folder}/{file}', 'r') as f:
                    metrics = f.read().split(',')
                    metrics_float = [float(x) for x in metrics]
                    if len(sum_metrics) == 0:
                        sum_metrics = metrics_float
                        for key in aggregate_metrics:
                            aggregate_metrics[key] = metrics_float
                    else:
                        aggregate_metrics['min'] = [min(tup) for tup in zip(
                            aggregate_metrics['min'], metrics_float)]
                        aggregate_metrics['max'] = [max(tup) for tup in zip(
                            aggregate_metrics['max'], metrics_float)]
                        sum_metrics = [sum(tup) for tup in zip(
                            sum_metrics, metrics_float)]
        aggregate_metrics['avg'] = [x / count for x in sum_metrics]

        all_experiments_metrics[index] = aggregate_metrics

    write_throughput_all_experiments(all_experiments_metrics)

    write_all_metrics_all_experiments(all_experiments_metrics)


def write_clients_csv(experiment_folders, nc_per_folder):
    with open('clients.csv', 'w') as w:
        for index, folder in enumerate(experiment_folders):
            nc = nc_per_folder[index]
            for i in range(1, nc + 1):
                with open(f'{folder}/{i}.metrics', 'r') as r:
                    metrics = r.read().split(',')
                    metrics_float = [float(x) for x in metrics]
                    metrics_rounded = ["{:.2f}".format(
                        x) for x in metrics_float]
                    metrics_str = ','.join(metrics_rounded)
                    result = f'{str(index + 5)},{i},{metrics_str}'
                    w.write(result)
                    w.write('\n')


def main():
    experiment_folders = ['run-20-node-4',
                          'run-20-node-5',
                          'run-40-node-4',
                          'run-40-node-5']
    nc_by_folder = [20, 20, 40, 40]

    # We use this to generate aggregated metrics for all performance benchmarks.
    # write_aggregate_metrics(experiment_folders)

    # We use this to write the metrics per client, as specified in the project brief.
    write_clients_csv(experiment_folders, nc_by_folder)


if __name__ == '__main__':
    main()
