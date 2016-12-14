import boto3

def compute_source_prefix(config):
    return '/'.join((
        config['s3']['prefix'],
        config['identity'],
    ))

def get_columnfamilies(config):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(config['s3']['bucket'])
    cfs = bucket.objects.filter(
        Prefix=compute_source_prefix(config),
        Delimiter=1000,
    )
    for i in cfs:
        print(i)

def download():
    config_filename = os.path.expanduser('~/backups.yaml')
    config = load_config(config_filename)
    get_columnfamilies(config)
