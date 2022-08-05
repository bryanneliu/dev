'''
kinit -f && mwinit -o; ada credentials update --account=802575742115 --provider=conduit --role AdminRole --profile=lbryanne --once

aws configure list --profile lbryanne

aws s3 ls --profile lbryanne
'''

'''
Upload local file to S3 folder:
aws s3 cp select_candidates.py s3://spear-team/lbryanne/synonyms/src/candidates/ --profile lbryanne
'''