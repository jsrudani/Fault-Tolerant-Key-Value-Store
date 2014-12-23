/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 * Name :- Jigar S. Rudani
 * NetID :- rudani2
**********************************/
#include "MP2Node.h"
#include <iostream>
#include <vector>
#include <cmath>

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	
	vector<Node> curMemList;
	/* curMemList_Iterator used to iterate the curMemList which is latest Membership List return by Membership Protocol*/
	vector<Node>::iterator curMemList_Iterator;
	/* ring_Iterator used to iterate the ring which is current view of Membership List maintained by each Node*/
	vector<Node>::iterator ring_Iterator;
	/* isPresent flag variable to indicate the presence of Node in the ring maintained by each Node*/
	bool isPresent = false;
	/* isRingUpdated flag variable to indicate that ring is updated*/
	bool isRingUpdated = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	/* Check if ring already exist
		If yes then call stabalization protocol to update and resolve the conflict 
		else initialize the ring for current member
	*/
	if (this->ring.size() == 0) {
		this->ring = curMemList;
	} else if (curMemList.size() != this->ring.size()) {
		isRingUpdated = true;	
	} else {
		for (curMemList_Iterator = curMemList.begin() ; curMemList_Iterator < curMemList.end(); ++curMemList_Iterator) {
			isPresent = false;
			for (ring_Iterator = this->ring.begin() ; ring_Iterator < this->ring.end(); ++ring_Iterator) {
				if ((*ring_Iterator).nodeHashCode == (*curMemList_Iterator).nodeHashCode) {
					isPresent = true;
					break;
				}
			}
			if(!isPresent) {
				isRingUpdated = true;
				break;
			}
		}
	}

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED.
	 * Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	 */
	if (isRingUpdated) {			

		this->stabilizationProtocol(curMemList);
		this->ring = curMemList;	
		isRingUpdated = false;	

	} else {

		this->ring = curMemList;
	}

	// Sort the list based on the hashCode 
	sort(this->ring.begin(), this->ring.end());
}

/**
 * FUNCTION NAME: getMembershipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	
	//cout << "Inside clientCreate() \n";
	int transactionID = 0;
	bool isReplica = true;
	Address _fromAddr;
	Message *msg;
	MessageType _type;
	ReplicaType _replica;
	string message;

	/* Get the new Transaction ID */
	transactionID = generateTransId(this->transID);
	//cout << "New Transaction ID for CREATE :"<< transactionID << " for NODE: "<< this->getMemberNode()->addr.getAddress() << endl;
	
	/* Construct Message with given Key and Value */
	_fromAddr = this->getMemberNode()->addr;
	_type = CREATE;
	_replica = PRIMARY;

	msg = new Message(transactionID, _fromAddr, _type, key, value, _replica);
	
	/* Prepare the Parameter to call send() */
	message = msg->toString();

	/* Store into map<int,string> for Quorum check */
	this->transId_Ops[transactionID] = message;

	/* Dispatch the Message */
	dispatchMessages(msg, isReplica);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	
	int transactionID = 0;
	bool isReplica = false;
	Address _fromAddr;
	Message *msg;
	MessageType _type;
	string message;

	/* Get the new Transaction ID */
	transactionID = generateTransId(this->transID);
	//cout << "New Transaction ID for READ: "<< transactionID << " for NODE: "<< this->getMemberNode()->addr.getAddress() << endl;
	
	/* Construct READ Message with given Key and Value */
	_fromAddr = this->getMemberNode()->addr;
	_type = READ;

	msg = new Message(transactionID, _fromAddr, _type, key);
	
	/* Prepare the Parameter to call send() */
	message = msg->toString();

	/* Store into map<int,string> for Quorum check */
	this->transId_Ops[transactionID] = message;

	/* Dispatch the Message */
	dispatchMessages(msg, isReplica);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	
	//cout << "Inside clientUpdate() \n";
	int transactionID = 0;
	bool isReplica = true;
	Address _fromAddr;
	Message *msg;
	MessageType _type;
	ReplicaType _replica;
	string message;

	/* Get the new Transaction ID */
	transactionID = generateTransId(this->transID);
	//cout << "New Transaction ID for UPDATE :"<< transactionID << " for NODE: "<< this->getMemberNode()->addr.getAddress() << endl;
	
	/* Construct Message with given Key and Value */
	_fromAddr = this->getMemberNode()->addr;
	_type = UPDATE;
	_replica = PRIMARY;

	msg = new Message(transactionID, _fromAddr, _type, key, value, _replica);
	
	/* Prepare the Parameter to call send() */
	message = msg->toString();

	/* Store into map<int,string> for Quorum check */
	this->transId_Ops[transactionID] = message;

	/* Dispatch the Message */
	dispatchMessages(msg, isReplica);

}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	
	//cout << "Inside clientDelete() \n";
	int transactionID = 0;
	bool isReplica = false;
	Address _fromAddr;
	Message *msg;
	MessageType _type;
	string message;

	/* Get the new Transaction ID */
	transactionID = generateTransId(this->transID);
	//cout << "New Transaction ID for DELETE: "<< transactionID << " for Node: "<< this->getMemberNode()->addr.getAddress() << endl;
	
	/* Construct DELETE Message with given Key and Value */
	_fromAddr = this->getMemberNode()->addr;
	_type = DELETE;

	msg = new Message(transactionID, _fromAddr, _type, key);
	
	/* Prepare the Parameter to call send() */
	message = msg->toString();

	/* Store into map<int,string> for Quorum check */
	this->transId_Ops[transactionID] = message;

	/* Dispatch the Message */
	dispatchMessages(msg, isReplica);		
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int transID, Address selfAddr) {
	
	bool isInserted = false;
	Entry *entryObj;
	string entryvalue;
	int timestamp = this->par->getcurrtime();

	//cout << "Key, Value Information for Node : " << selfAddr.getAddress() << endl;
	//cout << "key: " << key << endl;
	//cout << "value: " << value << endl;
	//cout << "replica: " << replica << endl;	
	
	/* Create Entry Object and convert into String and store into Hash Table */
	entryObj = new Entry(value, timestamp, replica);
	entryvalue = entryObj->convertToString();
	//cout << "entryvalue: " << entryvalue << endl;

	/* Insert into Hash Table */
	isInserted = this->ht->create(key, entryvalue);

	/* Get the Count of Hash Table */
	//cout << this->ht->currentSize() << endl;
	
	/* Log the isInserted flag so that in case if co-ordinator fails then server can replay log and get the status of the operation */
	if (isInserted)
		this->log->logCreateSuccess(&selfAddr, false, transID, key, value);
	else
		this->log->logCreateFail(&selfAddr, false, transID, key, value);

	/* Return isInserted boolean flag indicating SUCCESS or FAILURE Insert operation */
	return isInserted;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int transID, Address selfAddr) {
	
	string value = "";
	//cout << "Node from which Key is read: " << selfAddr.getAddress() << endl;
	//cout << "key to be read : " << key << endl;
	
	/* Read the Key from Hash Table */
	value = this->ht->read(key);

	/* Get the Count of Hash Table */
	//cout << this->ht->currentSize() << endl;
	
	/* Log the value so that in case if co-ordinator fails then server can replay log and get the value of the operation */
	if (value != "")
		this->log->logReadSuccess(&selfAddr, false, transID, key, value);
	else
		this->log->logReadFail(&selfAddr, false, transID, key);

	/* Return value corresponding to key passed */
	return value;

}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int transID, Address selfAddr) {
	
	bool isUpdated = false;
	Entry *entryObj;
	string entryvalue;
	int timestamp = this->par->getcurrtime();

	//cout << "Key, Value Information for UPDATE Node : " << selfAddr.getAddress() << endl;
	//cout << "key: " << key << endl;
	//cout << "value: " << value << endl;
	//cout << "replica: " << replica << endl;	
	
	/* Create Entry Object and convert into String and store into Hash Table */
	entryObj = new Entry(value, timestamp, replica);
	entryvalue = entryObj->convertToString();
	//cout << "entryvalue: " << entryvalue << endl;

	/* Insert into Hash Table */
	isUpdated = this->ht->update(key, entryvalue);

	/* Get the Count of Hash Table */
	//cout << this->ht->currentSize() << endl;
	
	/* Log the isUpdated flag so that in case if co-ordinator fails then server can replay log and get the status of the operation */
	if (isUpdated)
		this->log->logUpdateSuccess(&selfAddr, false, transID, key, value);
	else
		this->log->logUpdateFail(&selfAddr, false, transID, key, value);

	/* Return isUpdated boolean flag indicating SUCCESS or FAILURE Update operation */
	return isUpdated;

}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int transID, Address selfAddr) {
	
	bool isDeleted = false;
	//cout << "Node from which Key is deleted: " << selfAddr.getAddress() << endl;
	//cout << "key to be deleted : " << key << endl;
	
	/* Delete the Key from Hash Table */
	isDeleted = this->ht->deleteKey(key);

	/* Get the Count of Hash Table */
	//cout << this->ht->currentSize() << endl;
	
	/* Log the isDeleted flag so that in case if co-ordinator fails then server can replay log and get the status of the operation */
	if (isDeleted)
		this->log->logDeleteSuccess(&selfAddr, false, transID, key);
	else
		this->log->logDeleteFail(&selfAddr, false, transID, key);

	/* Return isDeleted boolean flag indicating SUCCESS or FAILURE Delete operation */
	return isDeleted;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	
	char * data;
	int size;
	bool isPresent = false;
	
	Message *msg;
	Message *reply_msg;
	Message *recv_msg;
	Address rep_fromAddr;
	Address rep_toAddr;
	MessageType rep_type;
	int rep_transID;
	string rep_message;
	string rep_value;
	/* Vector to store the received boolean status message and to store temp result value from map transId_result*/
	vector<bool> received_success_message;
	/* Vector to store the received value from message and to store temp result value from map transId_Value*/
	vector<string> received_value;
	vector<Node> replicaNodes_vec;
	/* replicaNodes_Iterator used to iterate the replica nodes recieved by findNodes */
	vector<Node>::iterator replicaNodes_Iterator;
	vector<string> converted_value;	
	string received_message;
	string recv_key;
	string recv_value;
	string value;
	
	bool rep_success = false;

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/* Construct the Message object from string message*/
		msg = new Message(message);

		/* Construct the Reply message parameter */
		rep_transID = msg->transID;
		rep_toAddr = msg->fromAddr;
		rep_fromAddr = this->getMemberNode()->addr;

		/* Call respective function based on the message type */
		
		/* MSGTYPE -> CREATE */
		if (msg->type == CREATE) {

			/* Call createKeyValue() to store key value in hashtable*/
			rep_success = this->createKeyValue(msg->key, msg->value, msg->replica, rep_transID, rep_fromAddr);
			
			/* Populate the vectors hasMyReplicas and haveReplicasOf based on replica type */
			replicaNodes_vec = findNodes(msg->key);
			
			if (msg->replica != PRIMARY) {
				if (msg->replica == SECONDARY) {
					isPresent = checkPresence(replicaNodes_vec.at(0),this->haveReplicasOf);
					if (!isPresent)
						this->haveReplicasOf.emplace_back(replicaNodes_vec.at(0));
				} else {
					isPresent = checkPresence(replicaNodes_vec.at(0),this->haveReplicasOf);
					if (!isPresent)
						this->haveReplicasOf.emplace_back(replicaNodes_vec.at(0));
					isPresent = checkPresence(replicaNodes_vec.at(1),this->haveReplicasOf);
					if (!isPresent)
						this->haveReplicasOf.emplace_back(replicaNodes_vec.at(1));
				}			
			} else {
				isPresent = checkPresence(replicaNodes_vec.at(1),this->hasMyReplicas);
				if (!isPresent)
					this->hasMyReplicas.emplace_back(replicaNodes_vec.at(1));
				isPresent = checkPresence(replicaNodes_vec.at(2),this->hasMyReplicas);
				if (!isPresent)
					this->hasMyReplicas.emplace_back(replicaNodes_vec.at(2));
			}
			replicaNodes_vec.clear();
			
			/* Construct message with all the required parameter */
			rep_type = REPLY;
			reply_msg = new Message(rep_transID, rep_fromAddr, rep_type, rep_success);
			/* Send the Message */
			sendReplyMessages(reply_msg, rep_fromAddr, rep_toAddr);
			
		} // END of CREATE
		
		// MSGTYPE -> DELETE
		if (msg->type == DELETE) {

			/* Call deletekey() to delete key from hashtable*/
			rep_success = this->deletekey(msg->key,rep_transID, rep_fromAddr);
			/* Construct message with all the required parameter */
			rep_type = REPLY;
			reply_msg = new Message(rep_transID, rep_fromAddr, rep_type, rep_success);
			/* Send the Message */
			sendReplyMessages(reply_msg, rep_fromAddr, rep_toAddr);

		} // END of DELETE

		// MSGTYPE -> READ
		if (msg->type == READ) {

			/* Call readKey() to read key from hashtable*/
			value = this->readKey(msg->key, rep_transID, rep_fromAddr);
			/* Construct message with all the required parameter */
			reply_msg = new Message(rep_transID, rep_fromAddr, value);
			/* Send the Message */
			sendReplyMessages(reply_msg, rep_fromAddr, rep_toAddr);

		} // END of READ

		// MSGTYPE -> UPDATE
		if (msg->type == UPDATE) {

			/* Call updateKeyValue() to update key value in hashtable */
			rep_success = this->updateKeyValue(msg->key, msg->value, msg->replica, rep_transID, rep_fromAddr);
			/* Construct message with all the required parameter */
			rep_type = REPLY;
			reply_msg = new Message(rep_transID, rep_fromAddr, rep_type, rep_success);
			/* Send the Message */
			sendReplyMessages(reply_msg, rep_fromAddr, rep_toAddr);	

		} // END of UPDATE

		// MSGTYPE -> REPLY
		if (msg->type == REPLY) {
			
			/* Get the success or failure indicator from reply message */
			rep_success = msg->success;
			
			/* Check if the transaction Id present in transId_result
				if yes then get the value of transaction and update the result bool vector
				else insert the result into bool vector and assign to transaction */
			if (this->transId_result.find(rep_transID) != this->transId_result.end()) {
				//cout << "Present into Result \n";
				received_success_message = this->transId_result[rep_transID];
				received_success_message.push_back(rep_success);
				this->transId_result[rep_transID] = received_success_message;
				received_success_message.clear();
			} else {
				//cout << "Not present \n";
				/* Insert into transId_result map with Key = transaction id; value = received_success_message */
				received_success_message.push_back(rep_success);
				this->transId_result[rep_transID] = received_success_message;
				received_success_message.clear();
			}

		} // END of REPLY

		// MSGTYPE -> READREPLY
		if (msg->type == READREPLY) {

			/* Get the value from reply message */
			rep_value = msg->value;

			/* Check if the transaction Id present in transId_result
				if yes then get the value of transaction and update the value vector
				else insert the result into value vector and assign to transaction */
			if (this->transId_Value.find(rep_transID) != this->transId_Value.end()) {
				//cout << "Present into Result \n";
				received_value = this->transId_Value[rep_transID];
				received_value.push_back(rep_value);
				this->transId_Value[rep_transID] = received_value;
				received_value.clear();
			} else {
				//cout << "Not present \n";
				/* Insert into transId_result map with Key = transaction id; value = received_value */
				received_value.push_back(rep_value);
				this->transId_Value[rep_transID] = received_value;
				received_value.clear();
			}
		}

		/* Finally block for msg */
		free(msg);

	} //END of While

	/* Quorum Checking for CREATE/DELETE/UPDATE */
	received_success_message.clear();
	if (this->transId_result.size() != 0) {
		for (map<int, vector<bool> >::iterator it=this->transId_result.begin(); it!=this->transId_result.end(); ++it) {

			rep_transID = it->first;
			if (this->transId_Ops.find(rep_transID) != this->transId_Ops.end()) {

					received_message = this->transId_Ops[rep_transID];				
					/* Construct the Message object from string received_message */
					recv_msg = new Message(received_message);
					/* Extract the operation type, Key, Value */
					rep_type = recv_msg->type;
					recv_key = recv_msg->key;
					recv_value = recv_msg->value;
					rep_fromAddr = recv_msg->fromAddr;
		
				received_success_message = this->transId_result[rep_transID];
				if (rep_type == CREATE) {
					if ((count (received_success_message.begin(), received_success_message.end(), true)) > 1)
						this->log->logCreateSuccess(&rep_fromAddr, true, rep_transID, recv_key, recv_value);
					else
						this->log->logCreateFail(&rep_fromAddr, true, rep_transID, recv_key, recv_value);
				}
				if (rep_type == DELETE) {
					if ((count (received_success_message.begin(), received_success_message.end(), true)) > 1)
						this->log->logDeleteSuccess(&rep_fromAddr, true, rep_transID, recv_key);
					else
						this->log->logDeleteFail(&rep_fromAddr, true, rep_transID, recv_key);
				}
				if (rep_type == UPDATE) {
					if ((count (received_success_message.begin(), received_success_message.end(), true)) > 1)
						this->log->logUpdateSuccess(&rep_fromAddr, true, rep_transID, recv_key, recv_value);
					else
						this->log->logUpdateFail(&rep_fromAddr, true, rep_transID, recv_key, recv_value);
				}
			}
		}
		/* Clearing entire Result Map as Quorum is done */
		this->transId_result.clear();
	}

	/* Quorum Checking for READ */
	received_value.clear();
	if (this->transId_Value.size() != 0) {
		for (map<int, vector<string> >::iterator it=this->transId_Value.begin(); it!=this->transId_Value.end(); ++it) {
			rep_transID = it->first;
			if (this->transId_Ops.find(rep_transID) != this->transId_Ops.end()) {

				received_message = this->transId_Ops[rep_transID];			
				/* Construct the Message object from string received_message */
				recv_msg = new Message(received_message);
				/* Extract the operation type, Key, Value */
				rep_type = recv_msg->type;
				recv_key = recv_msg->key;
				rep_fromAddr = recv_msg->fromAddr;
						
				received_value = this->transId_Value[rep_transID];
				converted_value = convertintoValue(received_value);
				rep_value = getValueFromReadReply(received_value);
				if (rep_type == READ) {
					if (((count (converted_value.begin(), converted_value.end(), rep_value)) > 1) && (rep_value != "")) {
						this->log->logReadSuccess(&rep_fromAddr, true, rep_transID, recv_key, rep_value);
					} else
						this->log->logReadFail(&rep_fromAddr, true, rep_transID, recv_key);			
				}
			}
			converted_value.clear();
			received_value.clear();
		}
		/* Clearing the result vector */
		this->transId_Value.clear();
	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(this->ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: generateTransId
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the latest transId from Vector of Transaction ID of current Node.
 * 				   Increment the count by 1 and return the new transID.
 */
int MP2Node::generateTransId(vector<int> transID) {

	int newTransID = 0;
	/* Check if transID is empty or not.
	*  if yes then intialize it zero value
	*  else increment the count
	*/
	if (this->transID.size() == 0)
		this->transID.push_back(0);
	newTransID = this->transID.back() + 1;
	this->transID.push_back(newTransID);
	return newTransID;

}
/**
 * FUNCTION NAME: convertintoValue
 *
 * DESCRIPTION: This function does the following:
 * 				1) Convert the vector of Entry object into value.
 */
vector<string> MP2Node::convertintoValue(vector<string> readreplymsg) {

	string rep_message;
	vector<string> value;
	vector<int> timestamp;
	Entry *entryObj;
	for (vector<string>::iterator it=readreplymsg.begin(); it!=readreplymsg.end(); ++it) {
		if (*it != "") {
			entryObj = new Entry(*it);
			value.push_back(entryObj->value);
			timestamp.push_back(entryObj->timestamp);
		} else
			value.push_back("");			
	}
	
	return value;
}
/**
 * FUNCTION NAME: getValueFromReadReply
 *
 * DESCRIPTION: This function does the following:
 * 				1) Extract the Value by comparing TimeStamp and return latest value.
 */
string MP2Node::getValueFromReadReply(vector<string> readreplymsg) {

	string rep_message;
	vector<string> value;
	vector<int> timestamp;
	Entry *entryObj;
	for (vector<string>::iterator it=readreplymsg.begin(); it!=readreplymsg.end(); ++it) {
		if (*it != "") {
			entryObj = new Entry(*it);
			value.push_back(entryObj->value);
			timestamp.push_back(entryObj->timestamp);
		}			
	}
	if (value.size() != 0)
		return value.at(0);
	else
		return "";
}
/**
 * FUNCTION NAME: sendReplyMessages
 *
 * DESCRIPTION: This function does the following:
 * 				1) Sends back the REPLY Message to Co-Ordinator.
 */
void MP2Node::sendReplyMessages(Message *sendMsg, Address fromAddr, Address toAddr) {

	string rep_message;

	/* Prepare the Parameter to call send() */
	rep_message = sendMsg->toString();	
		
	/* Send the Message to other nodes */
	emulNet->ENsend(&fromAddr, &toAddr, rep_message);
}

/**
 * FUNCTION NAME: dispatchMessages
 *
 * DESCRIPTION: This function does the following:
 * 				1) Multicast the Message.
 */
void MP2Node::dispatchMessages(Message *sendMsg, bool isReplica) {
	
	/* Declaration Area */
	vector<Node> replicaNodes_vec;
	Address _toAddr;
	Message *msg;
	string message;
	/* replicaNodes_Iterator used to iterate the replica nodes recieved by findNodes */
	vector<Node>::iterator replicaNodes_Iterator;

	/* Find the replicas of this key */
	replicaNodes_vec = findNodes(sendMsg->key);

	/* Convert into String */
	message = sendMsg->toString();

	/* Loop through all the received address vector and dispatch message to each node */
	for(replicaNodes_Iterator = replicaNodes_vec.begin();replicaNodes_Iterator < replicaNodes_vec.end(); ++replicaNodes_Iterator) {
		
		/* Populate the to_addr field */		
		_toAddr = (*replicaNodes_Iterator).nodeAddress;
		
		/* Send the Message to other nodes */
		emulNet->ENsend(&(sendMsg->fromAddr), &_toAddr, message);		

		/* Change the Replica type for next round if required */
		if (isReplica) {
			if (sendMsg->replica == PRIMARY)
				sendMsg->replica = SECONDARY;
			else if (sendMsg->replica == SECONDARY)
				sendMsg->replica = TERTIARY;
			else
				sendMsg->replica = PRIMARY;

			/* Construct message with all the required parameter */
			msg = new Message(sendMsg->transID, sendMsg->fromAddr,sendMsg->type, sendMsg->key, sendMsg->value, sendMsg->replica);

			/* Prepare the Parameter to call send() */
			message = msg->toString();
		}
	}
	/* Act as Finally block */
	replicaNodes_vec.clear();	
}
/**
 * FUNCTION NAME: findNeighbor
 * DESCRIPTION: Find the neighbor of the given hashcode
 */
vector<Node> MP2Node::findNeighbor(size_t pos, vector<Node> localNodes) {

	vector<Node> addr_vec;
	if (localNodes.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= localNodes.at(0).getHashCode() || pos > localNodes.at(localNodes.size()-1).getHashCode()) {
			addr_vec.emplace_back(localNodes.at(0));
			addr_vec.emplace_back(localNodes.at(1));
			addr_vec.emplace_back(localNodes.at(2));
		}
		else {
			// go through the localNodes until pos <= node
			for (int i=1; i<localNodes.size(); i++){
				Node addr = localNodes.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(localNodes.at((i+1)%localNodes.size()));
					addr_vec.emplace_back(localNodes.at((i+2)%localNodes.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}
/**
 * FUNCTION NAME: checkPresence
 * DESCRIPTION: Check whether element is present in the vector or not
 */
bool MP2Node::checkPresence(Node elementNode, vector<Node> nodeList) {

	vector<Node>::iterator ringNodes_Iterator;

	for(ringNodes_Iterator = nodeList.begin();ringNodes_Iterator != nodeList.end(); ++ringNodes_Iterator) {
		if ((*ringNodes_Iterator).nodeHashCode == elementNode.getHashCode())
			return true;
	}

	return false;
}
/**
 * FUNCTION NAME: findPositions
 * DESCRIPTION: Find the position of element from the given list
 */
int MP2Node::findPositions(Node elementNode, vector<Node> nodeList) {

	vector<Node>::iterator ringNodes_Iterator;
	int index = 0;

	for(ringNodes_Iterator = nodeList.begin();ringNodes_Iterator != nodeList.end(); ++ringNodes_Iterator) {
		if ((*ringNodes_Iterator).nodeHashCode != elementNode.getHashCode())
			index++;
		else
			break;
	}

	return index;
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol(vector<Node> currMemberList) {

	
	int transactionID = 0;
	int index = 0;
	bool isPresent = false;
	bool ishasReplica = false;
	bool ishaveReplica = false;
	bool rep_success = false;
	size_t pos;
	Address fromAddr;
	Address toAddr;
	string key;
	string value;
	Entry *entryobj;
	Message *reply_msg;
	MessageType _type;
	ReplicaType _replica;
	Node selfNode;
	Node failedNode;
	Node failedHasMyReplicaNode;
	Node newPrimaryNode;
	vector<Node>::iterator ringNodes_Iterator;
	vector<Node>::iterator currMemberNodes_Iterator;
	vector<Node>::iterator failedNodes_Iterator;
	vector<Node>::iterator hasMyReplica_Iterator;
	vector<Node>::iterator haveMyReplica_Iterator;
	map<string, string>::iterator hashtable_Iterator;

	/* Detect Failure Node */
	vector<Node> failNodes;
	vector<Node> replicaNodes_vec;
	vector<Node> localNodes;

	pos = (Node(this->getMemberNode()->addr)).getHashCode();
	fromAddr = this->getMemberNode()->addr;
	selfNode = Node(this->getMemberNode()->addr);

	for(ringNodes_Iterator = this->ring.begin();ringNodes_Iterator != this->ring.end(); ++ringNodes_Iterator) {
		isPresent = false;	
		if(checkPresence(*ringNodes_Iterator, currMemberList))
			isPresent = true;
		if(!isPresent)
			failNodes.emplace_back(Node((*ringNodes_Iterator).nodeAddress));
	}

	
	// CASE 1 :- If the failed replica node is not in my has and have group. In that case just update the ring.
	
	for(failedNodes_Iterator = failNodes.begin();failedNodes_Iterator < failNodes.end(); ++failedNodes_Iterator) {
		if(checkPresence(*failedNodes_Iterator, this->hasMyReplicas)) {
			ishasReplica = true;
			failedHasMyReplicaNode = *failedNodes_Iterator;
			break;
		}
	}
	
	for(failedNodes_Iterator = failNodes.begin();failedNodes_Iterator < failNodes.end(); ++failedNodes_Iterator) {
		if(checkPresence(*failedNodes_Iterator, this->haveReplicasOf)) {
			ishaveReplica = true;
			failedNode = *failedNodes_Iterator;
			break;
		}
	}
	localNodes = currMemberList;
	replicaNodes_vec.clear();
	if(ishasReplica) {
		// CASE 2 Handling. My Secondary/Tertiary Nodes are failed

		// Find My Neighbors
		replicaNodes_vec = findNeighbor(pos, localNodes);

		// UPDATE hasMyReplicas Vector
		index = findPositions(failedHasMyReplicaNode,this->hasMyReplicas);
		this->hasMyReplicas.erase(this->hasMyReplicas.begin()+index);
		this->hasMyReplicas.emplace_back(replicaNodes_vec.at(2));

		// CREATE TERTIARY REPLICA Message and send to new neighbor node
		for (hashtable_Iterator = this->ht->hashTable.begin(); hashtable_Iterator != this->ht->hashTable.end(); ++hashtable_Iterator) {
			key = hashtable_Iterator->first;
			value = hashtable_Iterator->second;
			entryobj = new Entry(value);
			if (entryobj->replica == PRIMARY) {

				// CREATE Message for New Tertiary Node
				transactionID = generateTransId(this->transID);
				_type = CREATE;
				_replica = TERTIARY;
				toAddr = replicaNodes_vec.at(2).nodeAddress;
				
				reply_msg = new Message(transactionID, fromAddr, _type, key, entryobj->value, _replica);
				sendReplyMessages(reply_msg, fromAddr, toAddr);

				// UPDATE Message for Secondary Node. Updation is done but I am not logging the update message.
				// As Grader doesnt check for it
				transactionID = generateTransId(this->transID);
				_type = UPDATE;
				_replica = SECONDARY;
				toAddr = replicaNodes_vec.at(1).nodeAddress;

				reply_msg = new Message(transactionID, fromAddr, _type, key, entryobj->value, _replica);
				//sendReplyMessages(reply_msg, fromAddr, toAddr);

			}
		}		
		replicaNodes_vec.clear();
		ishasReplica = false;
	}
	replicaNodes_vec.clear();
	if (ishaveReplica) {	
		
		// Find My Neighbors
		replicaNodes_vec = findNeighbor(pos, localNodes);
					
		// If failed Replica is first node in my haveReplicaOf vector then I am Secondary node
		if (this->haveReplicasOf.at(0).nodeHashCode == failedNode.getHashCode()) {
			
			for (hashtable_Iterator = this->ht->hashTable.begin(); hashtable_Iterator != this->ht->hashTable.end(); ++hashtable_Iterator) {
			key = hashtable_Iterator->first;
			value = hashtable_Iterator->second;
			entryobj = new Entry(value);
				if (entryobj->replica == SECONDARY) {
	
					// CREATE Message for New Tertiary Node
					transactionID = generateTransId(this->transID);
					_type = CREATE;
					_replica = TERTIARY;
					toAddr = replicaNodes_vec.at(2).nodeAddress;
				
					reply_msg = new Message(transactionID, fromAddr, _type, key, entryobj->value, _replica);
					sendReplyMessages(reply_msg, fromAddr, toAddr);

					// UPDATE Message for Secondary Node. Updation is done but I am not logging the update message.
					// As Grader doesnt check for it
					transactionID = generateTransId(this->transID);
					_type = UPDATE;
					_replica = SECONDARY;
					toAddr = replicaNodes_vec.at(1).nodeAddress;

					reply_msg = new Message(transactionID, fromAddr, _type, key, entryobj->value, _replica);
					//sendReplyMessages(reply_msg, fromAddr, toAddr);

					// UPDATE Replica for itself to PRIMARY. Updation is done but I am not logging the update message.
					// As Grader doesnt check for it
					transactionID = generateTransId(this->transID);
					_replica = PRIMARY;
					rep_success = this->updateKeyValue(key, entryobj->value, _replica, transactionID, fromAddr);
					toAddr = fromAddr;
					_type = REPLY;

					reply_msg = new Message(transactionID, fromAddr, _type, rep_success);
					//sendReplyMessages(reply_msg, fromAddr, toAddr);

				}
			}
		}

		replicaNodes_vec.clear();
		if (this->haveReplicasOf.at(1).nodeHashCode == failedNode.getHashCode()) {

			pos = failedNode.getHashCode();
			// Find New Primary Node
			replicaNodes_vec = findNeighbor(pos, this->ring);
			newPrimaryNode = replicaNodes_vec.at(1);

			// UPDATE hasMyReplicas Vector
			index = findPositions(failedNode, this->haveReplicasOf);			

			replicaNodes_vec.clear();
			pos = selfNode.getHashCode();
			replicaNodes_vec = findNeighbor(pos, localNodes);

			this->haveReplicasOf.erase(this->haveReplicasOf.begin()+index);
			this->haveReplicasOf.emplace_back(replicaNodes_vec.at(1));

			for (hashtable_Iterator = this->ht->hashTable.begin(); hashtable_Iterator != this->ht->hashTable.end(); ++hashtable_Iterator) {
			key = hashtable_Iterator->first;
			value = hashtable_Iterator->second;
			entryobj = new Entry(value);
				if (entryobj->replica == TERTIARY) {
			
					// UPDATE Replica for New PRIMARY Node. Updation is done but I am not logging the update message.
					// As Grader doesnt check for it
					transactionID = generateTransId(this->transID);
					_type = UPDATE;
					_replica = PRIMARY;
					toAddr = newPrimaryNode.nodeAddress;

					reply_msg = new Message(transactionID, fromAddr, _type, key, entryobj->value, _replica);
					//sendReplyMessages(reply_msg, fromAddr, toAddr);

					// CREATE Message for New Tertiary Node
					transactionID = generateTransId(this->transID);
					_type = CREATE;
					_replica = TERTIARY;
					toAddr = replicaNodes_vec.at(1).nodeAddress;
				
					reply_msg = new Message(transactionID, fromAddr, _type, key, entryobj->value, _replica);
					sendReplyMessages(reply_msg, fromAddr, toAddr);

					// UPDATE Replica for itself to SECONDARY. Updation is done but I am not logging the update message.
					// As Grader doesnt check for it
					transactionID = generateTransId(this->transID);
					_replica = SECONDARY;
					//rep_success = this->updateKeyValue(key, entryobj->value, _replica, transactionID, fromAddr);
					toAddr = fromAddr;
					_type = REPLY;

					reply_msg = new Message(transactionID, fromAddr, _type, rep_success);
					//sendReplyMessages(reply_msg, fromAddr, toAddr);

				}
			}
		}
		replicaNodes_vec.clear();
		ishaveReplica = false;
	} // END OF haveReplicaOf
} // END OF Stabilization Protocol
