/**********************************
 * FILE NAME: MP2Node.h
 * DESCRIPTION: MP2Node class header file
 * Name :- Jigar S. Rudani
 * NetID :- rudani2
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Vector boolean storing result of each operation
	vector<bool> result_ops;
	// Vector string value returned by READ operation
	vector<string> returned_value;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
	// Vector holding Transaction Id
	vector<int> transID;
	// Map holding Transaction ID and corresponding value returned by READ operation
	map<int, vector<string>> transId_Value;
	// Map holding Transaction ID and corresponding Operation for each transaction performed by Node
	map<int, string> transId_Ops;
	// Map holding Transaction ID and corresponding result for each Operation performed by transaction
	map<int, vector<bool>> transId_result;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message *message, bool isReplica);

	// Server sends the REPLY Message
	void sendReplyMessages(Message *sendMsg, Address fromAddr, Address toAddr);

	// Extract the Value by comparing TimeStamp and return latest value
	string getValueFromReadReply(vector<string> readreplymsg);
	
	// Convert the vector of Entry object into value
	vector<string> convertintoValue(vector<string> readreplymsg);

	// Find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);
	
	// Find the neighbor of the given hashcode from localNodes
	vector<Node> findNeighbor(size_t pos, vector<Node> localNodes);

	// Check whether element is present in the vector or not
	bool checkPresence(Node elementNode, vector<Node> nodeList);

	// Find the Position of element from the vector
	int findPositions(Node elementNode, vector<Node> nodeList);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica, int transID, Address selfAddr);
	string readKey(string key, int transID, Address selfAddr);
	bool updateKeyValue(string key, string value, ReplicaType replica, int transID, Address selfAddr);
	bool deletekey(string key, int transID, Address selfAddr);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol(vector<Node> memberList);

	// Generate Transaction ID
	int generateTransId(vector<int> transactionID);

	~MP2Node();
};

#endif /* MP2NODE_H_ */
