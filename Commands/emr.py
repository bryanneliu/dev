'''
kinit -f && mwinit -o; ada credentials update --account=802575742115 --provider=conduit --role AdminRole --profile=lbryanne --once

aws configure list --profile lbryanne

aws s3 ls --profile lbryanne

Remove the special characters in the command:
setopt noglob

The cloud desk and the EMR are in different regions. Use "--region"
aws emr add-steps --cluster-id j-36D5IBDJCFC3T --region us-east-1 

Put the program options at the end.
'''

aws emr add-steps \
    --cluster-id j-3UCLCKMXL0RUX \
    --region us-east-1 \
    --steps Type=CUSTOM_JAR, \
        Name="select_candidates", \
        ActionOnFailure=CONTINUE, \
        Jar=command-runner.jar, \
        Args=["spark-submit","--master","yarn","--py-files","s3://spear-team/guanzhe/commons.zip","s3://spear-team/lbryanne/synonyms/src/candidates/get_query_asins.py", "—output", "s3://spear-team/lbryanne/synonyms/data/","—region", "na"]
