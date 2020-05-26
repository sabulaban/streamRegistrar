import boto3
import json
import time
from datetime import datetime
from datetime import timezone
import pandas as pd
import os

class eventHanlder:
	def __init__(self, region, dur, src, stream, cname, sName, tenantID):
		self.region = region
		self.dur = dur
		self.src = src
		self.stream = stream
		self.sName = sName
		self.cname = cname
		self.stream_started = False
		self.lastSTime = 0
		self.lastETime = 0
		self.lastEventId = None
		self.initShardId = None
		self.incidentCache = None
		self.tenantID = tenantID
		self.init_stream()
		self.init_dynamo()
		if self.stream_started:
			self.poll_kinesis()
	def init_stream(self):
		self.stream_started = True
		try:
			self.client = boto3.client('kinesis', self.region)
		except:
			print('Consumer Failed to load @ boto3.client')
			self.stream_started = False			
		try:
			response = self.client.get_shard_iterator(StreamName=self.sName,
				ShardId='shardId-000000000000',
				ShardIteratorType='LATEST')
		except:
			print('Consumer Failed to load @ client.get_shard_iterator')
			self.stream_started = False
		if self.stream_started:
			self.initShardId = response['ShardIterator']

	def init_dynamo(self):

		try:
			dynamodb = boto3.resource('dynamodb')
		except:
			print('Dynamo DB not reachable')
		try:
			self.incidentsTable = dynamodb.Table('{}_incidents'.format(self.tenantID))
		except:
			print('Incidents table error link creation')
			self.incidentsTable = None
		try:
			self.alarmsTable = dynamodb.Table('{}_alarms'.format(self.tenantID))
		except:
			print('alarms table error link creation')
			self.alarmsTable = None
		try:
			self.nodesTable = dynamodb.Table('{}_nodes'.format(self.tenantID))
		except:
			print('nodes table error link creation')
			self.nodesTable = None
		try:
			self.fqasTable = dynamodb.Table('{}_fqas'.format(self.tenantID))
		except:
			print('nodes table error link creation')
			self.fqasTable = None
		try:
			self.rawTable = dynamodb.Table('{}_records'.format(self.tenantID))
		except:
			print('Raw Stream table error link creation')
			self.rawTable = None

	def batch_to_dynamo(self, records, table, phkeys):
		if table is not None:
			with table.batch_writer(overwrite_by_pkeys=phkeys) as batch:
				for record in records:
					record['source'] = self.src
					batch.put_item(Item = record)
		else:
			print('Table not availble')

	def flush(self, cache):
		alarms = []
		nodes = []
		fqas = []
		tStamps = []
		event_ids = []
		for record in cache:
			alarms.append(record['alarm'])
			nodes.append(record['node'])
			fqas.append(record['fqa'])
			tStamps.append(record['utcTStamp'])
			event_ids.append(record['event_id'])
		df = pd.DataFrame({'alarm': alarms, 'node': nodes, 'fqa': fqas, 'tStamp': tStamps, 'event_id': event_ids})
		aRecs = df.drop(['node', 'event_id', 'fqa'], axis = 1).groupby(['tStamp', 'alarm'], as_index=False).size().reset_index().rename(columns={0: "count"}).to_dict('records')
		nRecs = df.drop(['alarm', 'event_id', 'fqa'], axis = 1).groupby(['tStamp', 'node'], as_index=False).size().reset_index().rename(columns={0: "count"}).to_dict('records')
		fRecs = df.drop(['alarm', 'event_id', 'node'], axis = 1).groupby(['tStamp', 'fqa'], as_index=False).size().reset_index().rename(columns={0: "count"}).to_dict('records')
		eDf = df.drop(['alarm', 'fqa', 'node'], axis = 1).groupby(['event_id'], as_index=False).agg({'tStamp': ['min', 'max']}).reset_index()
		eDf.columns = eDf.columns.droplevel()
		eRecs = eDf.rename(columns={'': "event_id", 'min': 'tStamp', 'max': 'eTStamp'}).to_dict('records')
		t1 = time.time()
		for records, table, phkeys in zip([aRecs, nRecs, fRecs, eRecs, cache], [self.alarmsTable, self.nodesTable, self.fqasTable, self.incidentsTable, self.rawTable], [['tStamp', 'alarm'], ['node', 'tStamp'], ['fqa', 'tStamp'], ['event_id'], ['event_id', 'utcTStamp']]):
			self.batch_to_dynamo(records, table, phkeys)
		print(time.time() - t1)
		return None, []
	def poll_kinesis(self):
		shrdIt = self.initShardId
		cachedEventId = None
		cachedRecords = []
		while True:
			event = self.client.get_records(ShardIterator=shrdIt, Limit = 123)
			shrdIt = event['NextShardIterator']
			if len(event['Records']) == 0:
				dtInt = datetime.now()
				dtUTC = int(dtInt.replace(tzinfo=timezone.utc).timestamp())
				if ((dtUTC - self.lastETime) > self.dur) & (cachedEventId is not None):
#					print(cachedEventId, self.lastETime, self.lastSTime)
#					cachedEventId = None
#					alarms = []
#					nodes = []
#					fqas = []
#					tStamps = []
#					event_ids = []
#					for record in cachedRecords:
#						alarms.append(record['alarm'])
#						nodes.append(record['node'])
#						fqas.append(record['fqa'])
#						tStamps.append(record['utcTStamp'])
#						event_ids.append(record['event_id'])
#					df = pd.DataFrame({'alarm': alarms, 'node': nodes, 'fqa': fqas, 'tStamp': tStamps, 'event_id': event_ids})
#					aDf = pd.DataFrame(df.drop(['node', 'event_id', 'fqa'], axis = 1).groupby(['tStamp', 'alarm']).size())
#					nDf = pd.DataFrame(df.drop(['alarm', 'event_id', 'fqa'], axis = 1).groupby(['tStamp', 'node']).size())
#					fDf = pd.DataFrame(df.drop(['alarm', 'event_id', 'node'], axis = 1).groupby(['tStamp', 'fqa']).size())
#					print(aDf)
					cachedEventId, cachedRecords = self.flush(cachedRecords)
			if len(event['Records']) > 0:
				records = [json.loads(record['Data'].decode()) for record in event['Records']]
				records.sort(key = lambda item:item['utcTStamp'])
				unique_records = []
				retRecords = []
				for record in records:
					if record not in unique_records:
						unique_records.append(record)
						try:
							tStamp = record['utcTStamp']
						except:
							break
						if ((tStamp - self.lastETime) <= self.dur) & ((tStamp - self.lastETime) >= 0):
							record['event_id']  = 'event_{}'.format(str(self.lastSTime))
							record['trap_id'] = 'trap_{0}_{1}_{2}'.format(record['event_id'], record['fqa'], str(tStamp))
							self.lastETime = tStamp
							retRecords.append(record)
							cachedRecords.append(record)
						elif (tStamp - self.lastETime) > self.dur:
							if len(cachedRecords) > 0:
								cachedEventId, cachedRecords = self.flush(cachedRecords)
							self.lastSTime = tStamp
							self.lastETime = tStamp
							record['event_id']  = 'event_{}'.format(str(tStamp))
							record['trap_id'] = 'trap_{0}_{1}_{2}'.format(record['event_id'], record['fqa'], str(tStamp))
							self.lastEventId = 'event_{}'.format(str(tStamp))
							retRecords.append(record)
							cachedEventId = 'event_{}'.format(str(tStamp))
							cachedRecords.append(record)
						else:
							break

if __name__ == "__main__":
	# region, dur, src, stream, cname, sName
	streamname = os.environ['streamname']
	tenant = os.environ['tenant']
	region = os.environ['region']
	dur = int(os.environ['dur'])
	eh = eventHanlder(region, dur, 'kcl', 'whatever', '{}_kcl'.format(tenant), streamname, tenant)
	eh.poll_kinesis()

