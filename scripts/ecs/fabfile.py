from __future__ import with_statement
from fabric.api import  run, env, hide
from fabric.state import output
import tabulate
import termcolor
import boto3
import time
import json


# For usage options run:
#	fab --list
#
# Example usage:
# Run following from sandbox/scripts/
# Get info about all core-service apps running in prod:
# 	fab getDockerInstances:service=core-service,env=prod
# env is an optional parameter that defaults to test
#
# Get metrics for a specific service:
#	fab getMetrics:dockerId=2516acb605b8,ec2Host=10.0.11.54,service=shipment-pipeline
#
# Tail logs on a docker container:
#	fab tailLogs:dockerId=2516acb605b8,ec2Host=10.0.11.54,service=shipment-pipeline
#
# Defaults to tailing service logs. Set logType=request for the request logs
#	fab runCommand:dockerId=2516acb605b8,ec2Host=10.0.11.54,command="less /var/log/pivotfreight/shipment-pipeline/service.log"
#
# Run adhoc command on docker container:
#	fab runCommand:
# pip install the following packages:
# 	- fabric
#	- tabulate
#	- termcolor

class FabricException(Exception):
	pass

env.abort_exception = FabricException
env.gateway = 'pivotfreight@bastion-1:9254'
env.key_filename = ["/Users/aditya.kumar/.ssh/id_rsa_aws"]
env.use_ssh_config = True
output['status'] = False
output['aborts'] = False

adminPorts = {'address-service' : 8036,
    	      'event-persistor': 8156]


def _healthCheck(service, dockerId):
	try :
		response = run('docker exec -it %s bash -c \"curl localhost:%s/healthcheck\"' % (dockerId, adminPorts[service]))
		return "Healthy" if json.loads(response)['alive']['healthy'] else "Unhealthy"
	except FabricException:
		return "Unhealthy"

def _handleResponse(tableOutput, service, containerHost, response):
	for line in response.splitlines():
		if service in line:
			parts = line.split(' ')
			for part in parts:
				if service in part and 'amazon' in part:
					status = _healthCheck(service, parts[0])
					status = termcolor.colored(status, 'green') if (status == 'Healthy') else termcolor.colored(status, 'red')

					env = 'test'
					if 'prod' in part:
						env = 'prod'
					elif 'demo' in part:
						env = 'demo'

					tableOutput.append([parts[0], containerHost, status, env, part])

def _dockerList(tableOutput, service, ip):
	env.host_string = 'pivotfreight@' + ip
	with hide('output', 'running'):
		response = run('docker ps')
		_handleResponse(tableOutput, service, ip, response)

def getDockerInstances(service, env='test'):
	ecsClient = boto3.client('ecs')
	ec2Client = boto3.client('ec2')

	tableOutput = []

	response = ecsClient.list_container_instances(cluster=env, status='ACTIVE', maxResults=20)
	for containerInstanceArn in response['containerInstanceArns']:
		containerInstanceInfo = ecsClient.describe_container_instances(containerInstances=[containerInstanceArn.split('/')[1]], cluster=env)
		for containerInstance in containerInstanceInfo['containerInstances']:
			ec2InstanceId = containerInstance['ec2InstanceId']
			ec2Response = ec2Client.describe_instances(InstanceIds=[ec2InstanceId])
			for ec2Reservations in ec2Response['Reservations']:
				for instance in ec2Reservations['Instances']:
					print 'Searching EC2 Instance:' + ec2InstanceId + ' IP: ' +instance['PrivateIpAddress']
					_dockerList(tableOutput, service, instance['PrivateIpAddress'])
					time.sleep(5)

	print tabulate.tabulate(tableOutput,headers=['Docker ID', 'EC2 IP', 'Status', 'Env', 'Task'])

def getMetrics(service, dockerId, ec2Host):
	env.host_string = 'pivotfreight@' + ec2Host
	try :
		with hide('output', 'running'):
			response = run('docker exec -it %s bash -c \"curl localhost:%s/metrics\"' % (dockerId, adminPorts[service]))
			print json.dumps(json.loads(response), indent=4, sort_keys=True)
	except FabricException:
		print "Failed to get metrics."

def tailLogs(service, dockerId, ec2Host, logType='service'):
	env.host_string = 'pivotfreight@' + ec2Host
	env.output_prefix = False
	try :
		run('docker exec -it %s bash -c \"tail -f /var/log/pivotfreight/%s/%s.log\"' % (dockerId, service, logType))
	except FabricException:
		print "Failed to tail logs."

def runCommand(dockerId, ec2Host, command):
	env.host_string = 'pivotfreight@' + ec2Host
	env.output_prefix = False
	try :
		run('docker exec -it %s bash -c \"%s\"' % (dockerId, command))
	except FabricException:
		print "Failed to tail logs."



