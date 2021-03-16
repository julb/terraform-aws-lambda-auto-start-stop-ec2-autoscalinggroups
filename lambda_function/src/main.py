import os
import boto3

from base import LambdaFunctionBase


class CWScheduledEventManageEC2AutoScalingGroupsState(LambdaFunctionBase):
    """
    Class starting or stopping EC2 instances not part of a AutoScaling group.
    """

    # Section specific to the lambda.
    ACTION = os.environ['PARAM_ACTION']
    RESOURCE_TAG_KEY = os.environ['PARAM_RESOURCE_TAG_KEY']
    RESOURCE_TAG_VALUE = os.environ['PARAM_RESOURCE_TAG_VALUE']
    AWS_REGIONS = os.environ['PARAM_AWS_REGIONS'].split(',')

    def _get_ec2_auto_scaling_groups_by_tag(self, aws_region_name, tag_key, tag_value):
        """ Returns all resources identifiers linked to tag. """
        autoscaling_client = boto3.client('autoscaling', region_name=aws_region_name)

        # Finds EC2 instances.
        resource_pages = autoscaling_client.get_paginator('describe_auto_scaling_groups').paginate()

        # Browse EC2 instances and exclude EC2 member of a AutoScalingGroup.
        ec2_auto_scaling_groups = []
        for resource_page in resource_pages:
            for resource in resource_page['AutoScalingGroups']:
                for tag in resource['Tags']:
                    if tag['Key'] == tag_key and tag['Value'] == tag_value and len(resource['Instances']) > 0:
                        # EC2 instance IDs.
                        ec2_instance_ids = []
                        for instance in resource['Instances']:
                            ec2_instance_ids.append(instance['InstanceId'])

                        # Add ASG to the list.
                        ec2_auto_scaling_groups.append(
                            {
                                'name': resource['AutoScalingGroupName'],
                                'ec2_instance_ids': ec2_instance_ids
                            }
                        )

        return ec2_auto_scaling_groups

    def _stop_ec2_auto_scaling_groups(self, aws_region_name, ec2_auto_scaling_groups):
        """ Stop the EC2 Auto Scaling Groups. """
        autoscaling_client = boto3.client('autoscaling', region_name=aws_region_name)
        ec2_client = boto3.client('ec2', region_name=aws_region_name)

        self.logger.info('> Stopping EC2 AutoScaling groups.')
        for ec2_auto_scaling_group in ec2_auto_scaling_groups:
            self.logger.debug('>> Stopping AutoScaling Group %s.', ec2_auto_scaling_group['name'])

            # Suspend AutoScaling processes.
            autoscaling_client.suspend_processes(AutoScalingGroupName=ec2_auto_scaling_group['name'])

            # Stop Linked EC2 instances.
            for ec2_instance_id in ec2_auto_scaling_group['ec2_instance_ids']:
                self.logger.debug('>>> Stopping EC2: %s.', ec2_instance_id)
                ec2_client.stop_instances(
                    InstanceIds=[
                        ec2_instance_id
                    ]
                )

            # Auto Scaling Group stopped.
            self.logger.info('>> Auto Scaling Group %s => [STOPPED].', ec2_auto_scaling_group['name'])

    def _start_ec2_auto_scaling_groups(self, aws_region_name, ec2_auto_scaling_groups):
        """ Start the EC2 Auto Scaling Groups. """
        autoscaling_client = boto3.client('autoscaling', region_name=aws_region_name)
        ec2_client = boto3.client('ec2', region_name=aws_region_name)
        ec2_instance_running_waiter = ec2_client.get_waiter("instance_running")

        self.logger.info('> Starting EC2 AutoScaling groups.')
        for ec2_auto_scaling_group in ec2_auto_scaling_groups:
            self.logger.debug('>> Starting AutoScaling Group %s.', ec2_auto_scaling_group['name'])

            # Start Linked EC2 instances.
            for ec2_instance_id in ec2_auto_scaling_group['ec2_instance_ids']:
                self.logger.debug('>>> Starting EC2: %s.', ec2_instance_id)
                ec2_client.start_instances(
                    InstanceIds=[
                        ec2_instance_id
                    ]
                )

            # Waiting for EC2 to be all started.
            ec2_instance_running_waiter.wait(
                InstanceIds=ec2_auto_scaling_group['ec2_instance_ids'],
                WaiterConfig={'Delay': 15, 'MaxAttempts': 30},
            )

            # Resume AutoScaling processes.
            autoscaling_client.resume_processes(AutoScalingGroupName=ec2_auto_scaling_group['name'])

            # Auto Scaling Group started.
            self.logger.info('>> Auto Scaling Group %s => [STARTED].', ec2_auto_scaling_group['name'])

    def _execute(self, event, context):  # pylint: disable=W0613
        """ Execute the method. """
        self.logger.info('Starting the operation.')

        for aws_region_name in self.AWS_REGIONS:
            self.logger.info('> Searching Auto Scaling Groups in region %s having tag %s=%s.',
                             aws_region_name, self.RESOURCE_TAG_KEY, self.RESOURCE_TAG_VALUE)

            # Get EC2 by tag.
            ec2_auto_scaling_groups = self._get_ec2_auto_scaling_groups_by_tag(aws_region_name, self.RESOURCE_TAG_KEY, self.RESOURCE_TAG_VALUE)

            self.logger.info('> Found %s EC2 Auto Scaling Groups in region %s having tag %s=%s.',
                             str(len(ec2_auto_scaling_groups)), aws_region_name, self.RESOURCE_TAG_KEY, self.RESOURCE_TAG_VALUE)

            # Start/Stop
            if len(ec2_auto_scaling_groups) > 0:
                if self.ACTION in ['enable', 'start']:
                    self._start_ec2_auto_scaling_groups(aws_region_name, ec2_auto_scaling_groups)
                elif self.ACTION in ['disable', 'stop']:
                    self._stop_ec2_auto_scaling_groups(aws_region_name, ec2_auto_scaling_groups)

        self.logger.info('Operation completed successfully.')

        return self._build_response_ok()


def lambda_handler(event, context):
    """ Function invoked by AWS. """
    return CWScheduledEventManageEC2AutoScalingGroupsState().process_event(event, context)
