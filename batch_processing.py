import argparse
import subprocess

from datetime import datetime

if __name__ == '__main__':
    # Example run:
    # python batch_processing.py --start_year 2018 --start_month 2 --end_year 2020 --end_month 3 --job_type e
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_type", help="e for only extract, l for e and load and t for l and transform.", type=str, required=True)
    parser.add_argument("--start_year", help="Start year for batch job.", type=int, required=True)
    parser.add_argument("--start_month", help="Start month for batch job.", type=int, required=True)
    parser.add_argument("--end_year", help="End year for batch job.", type=int, required=True)
    parser.add_argument("--end_month", help="Start year for batch job.", type=int, required=True)

    args = parser.parse_args()

    start = datetime.now()

    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    if args.start_year == args.end_year:
        jobs = {str(args.start_year): months[args.start_month - 1:args.end_month]}
    else:
        jobs = {str(args.start_year):months[args.start_month-1:12],
            str(args.end_year):months[:args.end_month]}

        years_between= args.end_year - args.start_year - 1
        if years_between > 0:
            for i in range(years_between):
                jobs[str(args.start_year + i + 1)] = months

    print('This batch job will run for the folowing months!')

    c=0
    st=''
    for k in sorted([int(kk) for kk in jobs.keys()]):
        for m in jobs[str(k)]:
            st+= str(k) + '-' + m + ' ; '
            c+=1
    print(st)
    print(f'In total {c} monthes!')
    go = input('Do you want to continue?(y/n)')
    if go == 'n':
        print('Exiting!')
        exit(0)
    else:
        print('\nStarting the jobs: one after another ...\n')

    for k in sorted([int(kk) for kk in jobs.keys()]):
        for m in jobs[str(k)]:
            year= str(k)
            month =  m
            if args.job_type == 'e':
                print('Starting extract for ', year + '-' + month)
                process = subprocess.Popen(["python", "extract.py" , year , month])
                process.wait()
            elif args.job_type == 'l':
                print('Starting extract for ', year + '-' + month)
                process = subprocess.Popen(["python", "extract.py", year, month])
                process.wait()
                print('Starting load for ', year + '-' + month)
                process = subprocess.Popen(["python", "load.py"])
                process.wait()
            elif args.job_type == 't':
                print('Starting extract for ', year + '-' + month)
                process = subprocess.Popen(["python", "extract.py", year, month])
                process.wait()
                print('Starting load for ', year + '-' + month)
                process = subprocess.Popen(["python", "load.py"])
                process.wait()
                print('Starting transform for ', year + '-' + month)
                process = subprocess.Popen(["python", "transform.py"])
                process.wait()
            else:
                raise Exception('Wrong job_type; job_type can be e, l or t. Please change the argument accordingly.')
            print('Done ' + year + '-' + month)
            print('-----------')
    print('Time to complete:')
    print(st)
    print(datetime.now() - start)



