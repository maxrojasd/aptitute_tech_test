import subprocess
# Basic orchestrator to run scripts

if __name__ == '__main__':

    # Specify scripts to run
    scripts = ['tlc_scrapper.py', 'pyspark_code.py', 'unit_tests.py']

    # Try to Run scripts or send an error
    for script in scripts:
        try:
            subprocess.call(['python', script])
            print(f'Successfully ran {script}')

        except Exception as e:
            print(f'{script} failed. Traceback below: {str(e)}')
