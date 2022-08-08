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

'''
Upload local file to S3 folder:
aws s3 cp select_candidates.py s3://spear-team/lbryanne/synonyms/src/candidates/ --profile lbryanne
'''

'''
Submit EMR job using AWS CLI - example 1:
'''
aws emr add-steps --cluster-id j-28GSFLG1500UQ --region us-east-1 --steps Type=CUSTOM_JAR,Name="select_candidates",ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=["spark-submit","--master","yarn","--py-files","s3://spear-team/guanzhe/commons.zip","s3://spear-team/lbryanne/synonyms/src/candidates/select_candidates.py","--input","s3://synonym-expansion/data/na/20211014-0/probabilities/","--output","s3://spear-team/lbryanne/synonyms/data/candidates/","--num-partitions","24"] --profile lbryanne

(22-08-05 21:29:07) <0> [~/workplace/Synonyms/src/SpearSynonymExpansion/src/spear_synonym_expansion/candidates]  
dev-dsk-lbryanne-2b-80ed3fb7 % aws emr add-steps --cluster-id j-28GSFLG1500UQ --region us-east-1 --steps Type=CUSTOM_JAR,Name="select_candidates",ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=["spark-submit","--master","yarn","--py-files","s3://spear-team/guanzhe/commons.zip","s3://spear-team/lbryanne/synonyms/src/candidates/select_candidates.py","--input","s3://synonym-expansion/data/na/20211014-0/probabilities/","--output","s3://spear-team/lbryanne/synonyms/data/candidates/","--num-partitions","24"] --profile lbryanne

{
    "StepIds": [
        "s-2CTWTQQPRBTO9"
    ]
}

'''
Submit EMR job using AWS CLI - example 2:
'''
aws emr add-steps --cluster-id j-Y3PFMOQDFU4 --region us-east-1 --steps Type=CUSTOM_JAR,Name="get_qu_synonyms_statistics",ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=["spark-submit","--master","yarn","--py-files","s3://spear-team/lbryanne/synonyms/src/commons.zip","s3://spear-team/lbryanne/synonyms/src/synonyms_from_A9/get_qu_synonyms_statistics.py"] --profile lbryanne


aws emr add-steps --cluster-id j-Y3PFMOQDFU4 --region us-east-1 --steps Type=CUSTOM_JAR,Name="get_search_config_synonyms_statistics",ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=["spark-submit","--master","yarn","--py-files","s3://spear-team/lbryanne/synonyms/src/commons.zip","s3://spear-team/lbryanne/synonyms/src/synonyms_from_A9/get_search_config_synonyms_statistics.py"] --profile lbryanne

