from __future__ import with_statement
from fabric.api import run, env, hide
from fabric.state import output
from fabric.operations import prompt
import boto3

# For usage options run:
#	fab --list
#
# Example usage:
# 	fab printAlarm:alarmName='ADI TEST'
#	fab setSubscriptionForAlarmName:alarmName='ADI TEST',notificationArn='arn:aws:sns:us-east-1:369175392861:OpsGenie',actionType='InsufficientDataActions'
#	fab removeSubscriptionForAlarmName:alarmName='ADI TEST',notificationArn='arn:aws:sns:us-east-1:369175392861:OpsGenie',actionType='InsufficientDataActions'
#
# Run adhoc command on docker container:
#	fab runCommand:
# pip install the following packages:
# 	- fabric

class FabricException(Exception):
	pass

output['status'] = False
output['aborts'] = False

def getAlarms():
	cloudwatchClient = boto3.client('cloudwatch')

	response = cloudwatchClient.describe_alarms()
	while True:
		if 'NextToken' in response:
			nextToken = response['NextToken']
		else:
			nextToken = None

		for metricAlarm in response['MetricAlarms']:
			print metricAlarm['AlarmName']
			print metricAlarm['AlarmArn']
			print metricAlarm['AlarmActions']
			print metricAlarm['OKActions']
			print metricAlarm['InsufficientDataActions']
			print "------"

		if not nextToken:
			break
		else:
			response = cloudwatchClient.describe_alarms(NextToken=nextToken)


def printAlarm(alarmName):
	cloudwatchClient = boto3.client('cloudwatch')
	response = cloudwatchClient.describe_alarms(AlarmNames=[alarmName])
	metricAlarm = response['MetricAlarms'][0]
	print 'name: ' + metricAlarm['AlarmName']
	print 'alarmArn: ' + metricAlarm['AlarmArn']
	print 'alarmActions: ' + ' ,'.join(metricAlarm['AlarmActions'])
	print 'okActions: ' + ' ,'.join(metricAlarm['OKActions'])
	print 'insufficientDataActions: ' + ' ,'.join(metricAlarm['InsufficientDataActions'])

def updateSubscriptionForAllAlarms(notificationArn, actionType, set):
	cloudwatchClient = boto3.client('cloudwatch')

	response = cloudwatchClient.describe_alarms()
	keepGoing = True
	while True:
		if 'NextToken' in response:
			nextToken = response['NextToken']
		else:
			nextToken = None

		for metricAlarm in response['MetricAlarms']:
			if set == 'true':
				setAlarmSubscription(cloudwatchClient, metricAlarm, notificationArn, actionType)
			elif set == 'false':
				removeAlarmSubscription(cloudwatchClient, metricAlarm, notificationArn, actionType)

			keepGoing = prompt("Continue? (y/n)")
			if keepGoing != 'y':
				break


		if keepGoing != 'y':
			break

		if not nextToken:
			break
		else:
			response = cloudwatchClient.describe_alarms(NextToken=nextToken)

def getAllOpsGenieAlarms():
	cloudwatchClient = boto3.client('cloudwatch')

	response = cloudwatchClient.describe_alarms()
	count = 0;
	while True:
		if 'NextToken' in response:
			nextToken = response['NextToken']
		else:
			nextToken = None

		for metricAlarm in response['MetricAlarms']:
			if 'arn:aws:sns:us-east-1:369175392861:aws-alerts' in metricAlarm['AlarmActions']:
				count = count + 1
				print "--------------"
				print metricAlarm['AlarmName']
				print "AlarmActions: " + ' ,'.join(metricAlarm['AlarmActions'])
				print "OKActions: " + ' ,'.join(metricAlarm['OKActions'])
				print "InsufficientDataActions: " + ' ,'.join(metricAlarm['InsufficientDataActions'])
				print "--------------"

		if not nextToken:
			break
		else:
			response = cloudwatchClient.describe_alarms(NextToken=nextToken)

	print "Alarm count: %s" % count

def updateAlarm():
	cloudwatchClient = boto3.client('cloudwatch')
	alarm = cloudwatchClient.describe_alarms(AlarmNames=['ALARM NAME GOES HERE'])['MetricAlarms'][0]
	cloudwatchClient.put_metric_alarm(AlarmName=alarm['AlarmName'],
									  MetricName=alarm['MetricName'],
									  Namespace=alarm['Namespace'],
									  Period=alarm['Period'],
									  EvaluationPeriods=alarm['EvaluationPeriods'],
									  Threshold=alarm['Threshold'],
									  ComparisonOperator=alarm['ComparisonOperator'],
									  Statistic=alarm['Statistic'],
									  OKActions=alarm['OKActions'],
									  AlarmActions=alarm['AlarmActions'],
									  InsufficientDataActions=alarm['InsufficientDataActions'])
									  #AlarmDescription="Low disk space on prod Redshift cluster",
									  #TreatMissingData= 'missing',
									  #Dimensions=[ {
									  #	  "Name": "ClusterIdentifier",
									  #	  "Value": "us-east-1-prod-instance-1"
									  #},
									  #{
									  #	  "Name": "NodeID",
									  #	  "Value": "Shared"
									  #}])



def setSubscriptionForAlarmName(alarmName, notificationArn, actionType):
	cloudwatchClient = boto3.client('cloudwatch')
	alarm = cloudwatchClient.describe_alarms(AlarmNames=[alarmName])['MetricAlarms'][0]
	setAlarmSubscription(cloudwatchClient, alarm, notificationArn, actionType)

def setAlarmSubscription(cloudwatchClient, alarm, notificationArn, actionType):
	actions = alarm[actionType]
	if notificationArn not in actions:
		actions.append(notificationArn)
		putMetricAlarm(cloudwatchClient, alarm)
	else:
		print "Skipping adding notificationArn " + notificationArn + " for alarm " + alarm['AlarmName'] + " for actionType" + actionType

def removeSubscriptionForAlarmName(alarmName, notificationArn, actionType):
	cloudwatchClient = boto3.client('cloudwatch')

	alarm = cloudwatchClient.describe_alarms(AlarmNames=[alarmName])['MetricAlarms'][0]
	removeAlarmSubscription(cloudwatchClient, alarm, notificationArn, actionType)

def removeAlarmSubscription(cloudwatchClient, alarm, notificationArn, actionType):
	actions = alarm[actionType]
	if notificationArn in actions:
		actions.remove(notificationArn)
		putMetricAlarm(cloudwatchClient, alarm)
	else:
		print "Skipping removing notificationArn " + notificationArn + " for alarm " + alarm['AlarmName'] + " for actionType" + actionType


def putMetricAlarm(cloudwatchClient, alarm):
	if 'arn:aws:sns:us-east-1:369175392861:aws-alerts' not in alarm['AlarmActions']:
		print 'Skipping ' + alarm['AlarmName'] + ' because it its an unactionable alarm'
		return

	if any("autoscaling" in s for s in alarm['AlarmActions']):
		print 'Skipping ' + alarm['AlarmName'] + ' because its an autoscaling alarm'
		return

	if any("cluster" in s for s in alarm['AlarmActions']):
		print 'Skipping ' + alarm['AlarmName'] + ' because its a cluster alarm'
		return

	print 'Updating ' + alarm['AlarmName']
	if 'Statistic' in alarm:
		cloudwatchClient.put_metric_alarm(AlarmName=alarm['AlarmName'],
										  MetricName=alarm['MetricName'],
										  Namespace=alarm['Namespace'],
										  Period=alarm['Period'],
										  EvaluationPeriods=alarm['EvaluationPeriods'],
										  Threshold=alarm['Threshold'],
										  ComparisonOperator=alarm['ComparisonOperator'],
										  Statistic=alarm['Statistic'],
										  OKActions=alarm['OKActions'],
										  AlarmActions=alarm['AlarmActions'],
										  InsufficientDataActions=alarm['InsufficientDataActions'],
										  AlarmDescription=alarm['AlarmDescription'],
										  TreatMissingData= alarm['TreatMissingData'],
										  Dimensions=alarm['Dimensions'])
	elif 'ExtendedStatistic' in alarm:
		cloudwatchClient.put_metric_alarm(AlarmName=alarm['AlarmName'],
										  MetricName=alarm['MetricName'],
										  Namespace=alarm['Namespace'],
										  Period=alarm['Period'],
										  EvaluationPeriods=alarm['EvaluationPeriods'],
										  Threshold=alarm['Threshold'],
										  ComparisonOperator=alarm['ComparisonOperator'],
										  ExtendedStatistic=alarm['ExtendedStatistic'],
										  OKActions=alarm['OKActions'],
										  AlarmActions=alarm['AlarmActions'],
										  InsufficientDataActions=alarm['InsufficientDataActions'],
										  AlarmDescription=alarm['AlarmDescription'],
										  TreatMissingData= alarm['TreatMissingData'],
										  Dimensions=alarm['Dimensions'])

