'''
Previously I found my glue job covered by unknown script and does not generate expected output. Later it was found that all scripts are in a common S3 bucket and the default Script filename is Untitile job.py. That means if we forget to change the Script filename and if someone starts a new job and save, our job script will be covered by this new job script. The best practice is to point the script path to your specific folder and with a specific script filename. Thanks to Yi's help and sharing AWS CLI ways to create and start glue jobs.
https://docs.aws.amazon.com/cli/latest/reference/glue/create-job.html
https://docs.aws.amazon.com/cli/latest/reference/glue/start-job-run.html
'''

