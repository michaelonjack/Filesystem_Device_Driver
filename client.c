////////////////////////////////////////////////////////////////////////////////
//
//  File          : crud_client.c
//  Description   : This is the client side of the CRUD communication protocol.
//
//   Author       : Patrick McDaniel
//  Last Modified : Thu Oct 30 06:59:59 EDT 2014
//

// Include Files

// Project Include Files
#include <crud_network.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <arpa/inet.h>
#include <unistd.h>

// Global variables
int            crud_network_shutdown = 0; // Flag indicating shutdown
unsigned char *crud_network_address = NULL; // Address of CRUD server 
unsigned short crud_network_port = 0; // Port of CRUD server

// Global flag to determine if the client has connected to the server
uint8_t CONNECTED = 0;
int socket_fd;
struct sockaddr_in caddr;
	

//
// Functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_client_operation
// Description  : This the client operation that sends a request to the CRUD
//                server.   It will:
//
//                1) if INIT make a connection to the server
//                2) send any request to the server, returning results
//                3) if CLOSE, will close the connection
//
// Inputs       : op - the request opcode for the command
//                buf - the block to be read/written from (READ/WRITE)
// Outputs      : the response structure encoded as needed

CrudResponse crud_client_operation(CrudRequest op, void *buf) {

	uint8_t request; // Request type found from the opcode
	uint32_t length; // Length of the parameter buffer found from the opcode
	int bytesRead=0, bytesWritten=0; // Number of bytes read/written after calls to read/write
	char *tempBuf = NULL;

	// If the server hasn't been connected to yet, connect to the server
	if( !CONNECTED ) {
		
		// Set up address information
		caddr.sin_family = AF_INET;
		caddr.sin_port = htons(CRUD_DEFAULT_PORT);
		if( inet_aton( CRUD_DEFAULT_IP, &caddr.sin_addr) == 0 ) {
			return(-1);
		}

		socket_fd = socket(PF_INET,SOCK_STREAM,0);
		
		if( socket_fd == -1 ) {
			printf("Error on socket creation: %s \n", strerror(errno) );
			return(-1);
		}

		if( connect(socket_fd, (const struct sockaddr *)&caddr, sizeof(struct sockaddr)) == -1 ) {
			return(-1);
		}
		
		CONNECTED = 1; // Set flag to true once connected to server
	}

	request = (op << 32) >> 60; // Extract the request type from the opcode
	length = (op << 36) >> 40; // Extract the length of the parameter buffer from the opcode
	op = htonll64(op); // Convert opcode to network byte order

	// Send the opcode to the server
	if( write( socket_fd, &op, sizeof(op)) != sizeof(op) ) {
		// Print error if entire opcode was not written
		printf("Error writing network data (opcode): %s \n", strerror(errno) );
		return(-1);
	}

	// If the request is CREATE or UPDATE, send the buffer to the server in addition to the already sent opcode
	if( request == CRUD_CREATE || request == CRUD_UPDATE ) {
		
		tempBuf = malloc(length); // Allocate enough memory for tempBuf to hold what needs to be written
		memcpy(tempBuf,buf,length); // Copy what needs to be written into the temporary buffer

		// Continue writing bytes to the server until all bytes have been written
		while( bytesWritten != length ) {

			bytesWritten = bytesWritten + write( socket_fd, &tempBuf[bytesWritten], length-bytesWritten);
		}
	}
	
	// Receive the opcode from the server
	if( read( socket_fd, &op, sizeof(op)) != sizeof(op) ) {
		// Print error if the entire opcode was not read
		printf( "Error reading network data: %s \n", strerror(errno) );
		return(-1);
	}
	
	op = ntohll64(op); // Convert opcode to host byte order once it has been read from server
	request = (op << 32) >> 60; // Extract request type from the opcode
	length = (op << 36) >> 40; // Extract the length of the parameter buffer from the opcode

	// If the request is READ, receive buffer data from the server in addition to the already read opcode
	if( request == CRUD_READ ) {
		
		tempBuf = malloc(length); // Allocate enough memory for tempBuf to hold what needs to be read
		// memcpy(tempBuf,buf,length);

		while( bytesRead != length ) {
		
			bytesRead = bytesRead + read( socket_fd, &tempBuf[bytesRead], length-bytesRead );
		}
		memcpy(buf,tempBuf,length);
	}
	
	// If the request is CLOSE, close the connection between client and server
	if( request == CRUD_CLOSE ) {
		close(socket_fd);
		socket_fd = -1;
		CONNECTED = 0;
	}

	// If the temporary buffer was used, free the memory it allocated
	if(tempBuf) {
		free(tempBuf);
		tempBuf = NULL;	
	}

	return op;
}
