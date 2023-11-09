import subprocess

if __name__ == '__main__':

    # Specify unit tests to run
    scripts = ['unit_tests/func_utils_unit_test.py', 'unit_tests/pyspark_utils_unit_test.py']

    # Try to Run scripts or send an error
    for script in scripts:
        try:
            subprocess.call(['python', script])
            print(f'Successfully ran {script} unit test')

        except Exception as e:
            print(f'Unit test {script} failed. Traceback below: {str(e)}')

            