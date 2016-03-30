import boto3
import datetime
import logging

"""
This function lists all instances that have the `MakeSnapshot` tag and performs
a snapshot of all EBS volumes attached to it. It also tags the snapshots for
deletion after a number of retention days.
"""

MIN_RETENTION_COUNT = 1
RETENTION_DAYS_DEFAULT = 2
SNAPSHOT_TAG = 'MakeSnapshot'
RETENTION_TAG = 'Retention'
DELETE_TAG = 'AutoDelete'

ec2 = boto3.resource('ec2')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info('AWS snapshot backups started for event [%s]', event)
    instances = ec2.instances.filter(Filters=[
        {'Name': 'tag-key', 'Values': [SNAPSHOT_TAG]},
        {'Name': 'instance-state-name', 'Values': ['running']},
    ])
    for instance in instances:
        instance_tags = tags_dict(instance.tags)
        make_snapshot = str(instance_tags.get(SNAPSHOT_TAG)).lower()
        if make_snapshot in ('false', '0', 'none'):
            logger.info('Skip instance [%s]: [%s] tag value is false',
                instance.id, SNAPSHOT_TAG)
            continue
        instance_name = instance_tags.get('Name', instance.id)
        logger.info('Instance [%s]: [%s]', instance.id, instance_name)
        retention_days = int(instance_tags.get(RETENTION_TAG, RETENTION_DAYS_DEFAULT))
        for volume in instance.volumes.all():
            volume_tags = tags_dict(volume.tags)
            make_snapshot = str(volume_tags.get(SNAPSHOT_TAG, True)).lower()
            if make_snapshot in ('false', '0', 'none'):
                logger.info('\tSkip volume [%s]: [%s] tag value is false',
                    volume.id, SNAPSHOT_TAG)
                continue
            logger.info('\tVolume [%s]', volume.id)
            snapshot_date = datetime.datetime.utcnow().isoformat().rpartition('.')[0]
            description = '{}.{}.{}'.format(instance_name, volume.id, snapshot_date)
            new_snapshot = volume.create_snapshot(Description=description)
            new_snapshot.create_tags(Tags=[{'Key': DELETE_TAG, 'Value': 'true'}])
            logger.info('\t\tSnapshot created with description [%s]', description)
            volume_retention_days = int(volume_tags.get(RETENTION_TAG, retention_days))
            snapshots = volume.snapshots.filter(Filters=[
                {'Name': 'tag-key', 'Values': [DELETE_TAG]},
            ])
            snapshots = list(snapshots)
            if len(snapshots) <= MIN_RETENTION_COUNT:
                logger.info('\t\tSkip deletion of snapshots: count [%d] <= minimum retention count [%d]',
                    len(snapshots), MIN_RETENTION_COUNT)
                continue
            for snapshot in snapshots:
                snapshot_tags = tags_dict(snapshot.tags)
                auto_delete = str(snapshot_tags.get(DELETE_TAG)).lower()
                if auto_delete != 'true':
                    logger.info('\t\tSkip deletion of snapshot [%s]: [%s] tag value is not ["true"]',
                        snapshot.id, DELETE_TAG)
                    continue
                tz = snapshot.start_time.tzinfo
                snapshot_retention = datetime.timedelta(days=volume_retention_days)
                is_expired = (datetime.datetime.now(tz) - snapshot.start_time) > snapshot_retention
                if not is_expired:
                    logger.info('\t\tSkip deletion of snapshot [%s]: still within retention period of [%d] days',
                        snapshot.id, volume_retention_days)
                    continue
                logger.info('\t\tDelete snapshot [%s]: [%s]', snapshot.id, snapshot.description)
                snapshot.delete()
    logger.info('AWS snapshot backups completed')
    return True


def tags_dict(tags):
        '''Flatten a list of tags dicts into a single dict'''
        return {t['Key']: t['Value'] for t in tags} if tags else {}
