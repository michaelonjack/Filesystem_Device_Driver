////////////////////////////////////////////////////////////////////////////////
//
//  File           : crud_file_io.h
//  Description    : This is the implementation of the standardized IO functions
//                   for used to access the CRUD storage system.
//
//  Author         : Michael Onjack
//  Last Modified  : Mon Oct 20 12:38:05 PDT 2014
//

// Includes
#include <malloc.h>
#include <string.h>

// Project Includes
#include <crud_file_io.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <crud_network.h>

// Defines
#define CIO_UNIT_TEST_MAX_WRITE_SIZE 1024
#define CRUD_IO_UNIT_TEST_ITERATIONS 10240

// Other definitions

// Type for UNIT test interface
typedef enum {
	CIO_UNIT_TEST_READ   = 0,
	CIO_UNIT_TEST_WRITE  = 1,
	CIO_UNIT_TEST_APPEND = 2,
	CIO_UNIT_TEST_SEEK   = 3,
} CRUD_UNIT_TEST_TYPE;

// File system Static Data
// This the definition of the file table
CrudFileAllocationType crud_file_table[CRUD_MAX_TOTAL_FILES]; // The file handle table

// Pick up these definitions from the unit test of the crud driver
CrudRequest construct_crud_request(CrudOID oid, CRUD_REQUEST_TYPES req,
		uint32_t length, uint8_t flags, uint8_t res);
int deconstruct_crud_request(CrudRequest request, CrudOID *oid,
		CRUD_REQUEST_TYPES *req, uint32_t *length, uint8_t *flags,
		uint8_t *res);

//
// Implementation

// Global flag to determine if the object store has been initialized
uint8_t INITIALIZED = 0;

////////////////////////////////////////////////////////////////////////////////
//
// Function     : create_crud_request
// Description  : Forms the 64 bit integer request that will be passed to the crud_client_operation function
//
// Inputs       : object_id - identifying number of the object associated with the file
//		  req - request type of the command we are trying to execute
//		  len - the length of the object
//		  flag - currently unused for this assignment
//		  rslt - result code used in the response (0=success, 1=failure)
// Outputs      : 64 bit integer request to use in crud_client_operation

CrudRequest create_crud_request(uint32_t object_id, uint8_t req, uint32_t len, uint8_t flag, uint8_t rslt) {
	
	uint64_t crud_request = 0;
	
	crud_request = ((uint64_t)object_id) << 32;
	crud_request = crud_request | (((uint64_t)req) << 28);
	crud_request = crud_request | (((uint64_t)len) << 4);
	crud_request = crud_request | (((uint64_t)flag) << 1);
	crud_request = crud_request | ((uint64_t)rslt);
	
	return crud_request;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : extract_crud_response
// Description  : Deconstructs the response values from the 32 bit number returned from CRUD operations
//
// Inputs       : response - the response from the bus request function to extract values from
//		  responseValues - array of the individual values that make up the response
// Outputs      : Array containing the individual values of crud response

void extract_crud_response(CrudResponse response, uint32_t *object_id, uint8_t *request, uint32_t *length, uint8_t *flag, uint8_t *result) {
	
	// Shift forward to clear irrevelant bits to zero, then shift back to get individual value
	(*object_id) = response >> 32; // Object ID
	(*request) = (response << 32) >> 60; // Request
	(*length) = (response << 36) >> 40; // Length
	(*flag) = (response << 60) >> 61; // Flag
	(*result) = (response << 63) >> 63; // Result
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_format
// Description  : This function formats the crud drive, and adds the file
//                allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_format(void) {
	
	int i, priorityOID=0; // The object id of the priority object
	int prioritySize = sizeof(CrudFileAllocationType)*CRUD_MAX_TOTAL_FILES; // Size of priority object
	uint8_t req, result=1, flag=0; // variables needed for extract_crud_response function
	uint32_t id, length; // variables needed for extract_crud_response function 
	void *buf = malloc(prioritySize); // buffer to hold file allocation table
	CrudResponse response;
	CrudRequest request;

	// Determine if the object store has been initialized yet
	if( !INITIALIZED ) {
		
		// Initialize object store
		request = create_crud_request(0, CRUD_INIT, 0, 0, 0);
		response = crud_client_operation(request, NULL);
		
		// Check for CRUD command success
		extract_crud_response(response, &id, &req, &length, &flag, &result);
		
		if( result ) 
			return -1; // ERROR - result code is 1 meaning there was a failure 
		
		// Set INITIALIZED to true to show the object store has now been initialized
		INITIALIZED = 1;
	}

	// Delete crud_content.crd and all objects in the object store
	request = create_crud_request(0, CRUD_FORMAT, 0, CRUD_NULL_FLAG, 0);
	response = crud_client_operation(request, NULL);

	// Check for CRUD command success
	extract_crud_response(response, &id, &req, &length, &flag, &result);
	if( result )
		return -1; // ERROR - result code is 1 meaning there was a failure 
	
	// Initialize the file allocation table with all zeros
	for( i=0; i<CRUD_MAX_TOTAL_FILES; i++ ) {
		
		memset(crud_file_table[i].filename,0,CRUD_MAX_PATH_LENGTH);
		crud_file_table[i].object_id = CRUD_NO_OBJECT;
		crud_file_table[i].position = 0;
		crud_file_table[i].length = 0;
		crud_file_table[i].open = 0;
	}

	// Copy table data into buf
	memcpy(buf,crud_file_table,prioritySize);
	// Create priority object containing the table data
	request = create_crud_request(priorityOID, CRUD_CREATE, prioritySize, CRUD_PRIORITY_OBJECT, 0);
	response = crud_client_operation(request, buf);

	// Check for CRUD command success
	extract_crud_response(response, &id, &req, &length, &flag, &result);
	if( result )
		return -1; // ERROR - result code is 1 meaning there was a failure

	free(buf);
	buf = NULL;

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... formatting complete.");
	return(0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_mount
// Description  : This function mount the current crud file system and loads
//                the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_mount(void) {
	
	int priorityOID=0; // The object id of the priority object
	int tableSize = sizeof(CrudFileAllocationType)*CRUD_MAX_TOTAL_FILES;
	uint8_t req, result=1, flag=0; // variables needed for extract_crud_response function
	uint32_t id, length; // variables needed for extract_crud_response function 
	void *buf = malloc(tableSize); // Buffer to hold read priority object
	CrudRequest request;
	CrudResponse response;

	// Determine if the object store has been initialized yet
	if( !INITIALIZED ) {
		// Initialize object store
		request = create_crud_request(0, CRUD_INIT, 0, 0, 0);
		response = crud_client_operation(request, NULL);

		// Check for CRUD command success
		extract_crud_response(response, &id, &req, &length, &flag, &result);
		if( result )
			return -1; // ERROR - result code is 1 meaning there was a failure 
		
		// Set INITIALIZED to true to show the object store has now been initialized
		INITIALIZED = 1;
	}

	request = create_crud_request(priorityOID, CRUD_READ, tableSize, CRUD_PRIORITY_OBJECT, 0);
	response = crud_client_operation(request, buf);

	// Check for CRUD command success
	extract_crud_response(response, &id, &req, &length, &flag, &result);
	if( result )
		return -1; // ERROR - result code is 1 meaning there was a failure

	// Copy contents of the file allocation table read from the priority object into crud_file_table structure
	memcpy(crud_file_table,buf,tableSize);

	free(buf);
	buf = NULL;

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... mount complete.");
	return(0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_unmount
// Description  : This function unmounts the current crud file system and
//                saves the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_unmount(void) {
	
	int priorityOID=0; // The object id of the priority object
	int tableSize = sizeof(CrudFileAllocationType)*CRUD_MAX_TOTAL_FILES;
	uint8_t req, result=1, flag=0; // variables needed for extract_crud_response function
	uint32_t id, length; // variables needed for extract_crud_response function 
	void *buf = malloc(tableSize); // Buffer to hold read priority object
	memcpy(buf,crud_file_table,tableSize); // Copy file table contents to buffer
	CrudRequest request;
	CrudResponse response;
	
	// Update the priority object with the current file table
	request = create_crud_request(priorityOID, CRUD_UPDATE, tableSize, CRUD_PRIORITY_OBJECT, 0);
	response = crud_client_operation(request, buf);

	// Check for CRUD command success
	extract_crud_response(response, &id, &req, &length, &flag, &result);
	if( result )
		return -1; // ERROR - result code is 1 meaning there was a failure in command execution

	request = create_crud_request(0, CRUD_CLOSE, 0, 0, 0);
	response = crud_client_operation(request, NULL);

	// Check for CRUD command success
	extract_crud_response(response, &id, &req, &length, &flag, &result);
	if( result )
		return -1; // ERROR - result code is 1 meaning there was a failure in command execution

	free(buf);
	buf = NULL;

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... unmount complete.");
	return (0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_open
// Description  : This function finds an unopen file and returns its file handle
//
// Inputs       : path - the path "in the storage array"
// Outputs      : file handle if successful, -1 if failure

int16_t crud_open(char *path) {
	
	int i, fileExists=0; // Boolean flag to determine if the requested file exists
	int existingFd; // File descriptor of the existing file if it is found
	uint8_t req, result=1, flag=0; // variables needed for extract_crud_response function
	uint32_t occupy; // Meaningless variable needed in order to use the extract_crud_response function
	CrudResponse response;
	CrudRequest request;

	// Determine if the object store has been initialized yet
	if( !INITIALIZED ) {
		// Initialize object store
		request = create_crud_request(0, CRUD_INIT, 0, 0, 0);
		response = crud_client_operation(request, NULL);

		// Check for CRUD command success
		extract_crud_response(response, &occupy, &req, &occupy, &flag, &result);
		if( result )
			return -1; // ERROR - result code is 1 meaning there was a failure 
		
		// Set INITIALIZED to true to show the object store has now been initialized
		INITIALIZED = 1;
	}

	// Search for existing file in file table with the same name as parameter 'path'
	for( i=0; i<CRUD_MAX_TOTAL_FILES; i++ ) {
		// Test if the current file in the iteration has a name
		if( strcmp(crud_file_table[i].filename,"") != 0 ) {
			// Test if the current file's name matches 'path'
			if( strncmp( crud_file_table[i].filename, path, strlen(path)) == 0 ) {
				fileExists = 1; // Set fileExists flag to true
				existingFd = i; // Set the existing file's descriptor to be its index in the table
			}
		}
	}

	// CASE 1: File specified by 'path' exists but is simply closed
	// If the requested file exists, set file to open and set position to the beginning of the file
	if( fileExists ) {

		crud_file_table[existingFd].open = 1;
		crud_file_table[existingFd].position = 0;
		return existingFd;
	}

	// CASE 2: File specified by 'path' does not exist
	// If the file described by 'path' does not exist, create a new one
	else {
		for( i=0; i<CRUD_MAX_TOTAL_FILES; i++ ) {

			// Find unopen file that does not already exist
			if( !crud_file_table[i].open && strcmp(crud_file_table[i].filename,"")==0 ) {
				
				// Show that the file has been opened
				crud_file_table[i].open = 1;

				// Give initial values to other file variables
				strcpy( crud_file_table[i].filename, path); // make filename the parameter path
				crud_file_table[i].object_id = CRUD_NO_OBJECT;
				crud_file_table[i].length = 0;
				crud_file_table[i].position = 0;

				// Return the new file handle for the file that has been opened
				return i;
			}
		}

		return -1; // ERROR - End of for loop reached without finding a unopen file
	}
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_close
// Description  : This function closes the file
//
// Inputs       : fd - the file handle of the object to close
// Outputs      : 0 if successful, -1 if failure

int16_t crud_close(int16_t fd) {
	
	if( fd < 0 || fd > CRUD_MAX_TOTAL_FILES-1 )
		return -1; // ERROR - requested file handle out of range

	if( !crud_file_table[fd].open )
		return -1; // ERROR - requested file has not yet been opened

	// Set the file to closed
	crud_file_table[fd].open = 0;

	return 0;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_read
// Description  : Reads up to "count" bytes from the file handle "fh" into the
//                buffer  "buf".
//
// Inputs       : fd - the file descriptor for the read
//                buf - the buffer to place the bytes into
//                count - the number of bytes to read
// Outputs      : the number of bytes read or -1 if failures
// 
int32_t crud_read(int16_t fd, void *buf, int32_t count) {

	// Temporary buffer used to read the entire object associated with the file
	char *tempBuf;
	// Temporary buffer used to hold only what needs to be read
	char *tempBuf2;

	int32_t i, bytesRead=0;
	uint8_t req, result=1, flag=0;
	CrudRequest request;
	CrudResponse response;
	
	if( fd < 0 || fd > CRUD_MAX_TOTAL_FILES-1 )
		return -1; // ERROR - requested file handle out of range

	if( !crud_file_table[fd].open )
		return -1; // ERROR - requested file has not yet been opened
	if( buf == NULL )
		return -1; // ERROR - buffer doesn't point to meaningful data
	if( crud_file_table[fd].length == 0 )
		return bytesRead; // No bytes to read (length = 0) so return 0
	if( count < 1 )
		return bytesRead; // If no bytes are to be read (count = 0), return 0

	// Allocate enough memory to store the bytes of the current file
	tempBuf = malloc(CRUD_MAX_OBJECT_SIZE);
	// Allocate enough memory to store the bytes that will be read
	tempBuf2 = malloc(count);

	// Read the contents of the requested file into temporary buffer tempBuf
	request = create_crud_request(crud_file_table[fd].object_id, CRUD_READ, CRUD_MAX_OBJECT_SIZE, 0, 0);
	response = crud_client_operation(request, tempBuf);

	extract_crud_response(response, &crud_file_table[fd].object_id, &req, &crud_file_table[fd].length, 
		&flag, &result);
	if( result )
		return -1; // ERROR - result code is 1 meaning there was a failure in command execution

	// While the position in the current file does not exceed its length AND "count" bytes have not been read..
	for( i=0; crud_file_table[fd].position < crud_file_table[fd].length && i<count; i++ ) {
		// Copy the bytes one at a time from one buffer to the other starting at the position in the file
		tempBuf2[i] = tempBuf[crud_file_table[fd].position];
		crud_file_table[fd].position++; // Increment the file's position as each byte is read
		bytesRead++; // Increment the number of bytes read
	}

	// Copy what needed to be read into the parameter buffer
	memcpy(buf,tempBuf2,bytesRead);

	// Free the memory allocated by the temporary buffers if they are not pointing to NULL and reset to NULL
	if( tempBuf ) {
		free(tempBuf);
		tempBuf = NULL;
	}
	if( tempBuf2 ) {
		free(tempBuf2);
		tempBuf2 = NULL;
	}

	return bytesRead;
}

//////////////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_write
// Description  : Writes "count" bytes to the file handle "fh" from the
//                buffer  "buf"
//
// Inputs       : fd - the file descriptor for the file to write to
//                buf - the buffer to write
//                count - the number of bytes to write
// Outputs      : the number of bytes written or -1 if failure
// Write count bytes at the end of the current file position
// If you write past end of the file, you increase the file size
// 
int32_t crud_write(int16_t fd, void *buf, int32_t count) {

	// Temporary buffer used to hold the bytes that need to be written
	char *tempBuf;
	// Temporary buffer used to hold the bytes that are currently in the file
	char *tempBuf2;

	int newLength; // Length of the new object when resizing is needed
	uint8_t req, result=1, flag=0; // variables needed for extract_crud_response function
	CrudRequest request;
	CrudResponse response;
	
	if( fd < 0 || fd > CRUD_MAX_TOTAL_FILES-1 )
		return -1; // ERROR - file handle out of range
	if( !crud_file_table[fd].open )
		return -1; // ERROR - requested file has not yet been opened
	if( buf == NULL )
		return -1; // ERROR - buffer doesn't point to meaningful data
	if( count < 1 )
		return 0; // No bytes are to be written from the buffer

	// Allocate enough memory to store the bytes that need to be written
	tempBuf = malloc(count);
	if( tempBuf == NULL )
		return -1; // ERROR - malloc returned a NULL pointer

	tempBuf2 = NULL;

	// Copy the bytes that need to be written to the temporary buffer "tempBuf"
	memcpy(tempBuf, buf, count);

	// CASE 0: The current file has not yet been associated with an object
	// If the file hasn't been associated with an object yet, create the object and store the bytes in it
	if( crud_file_table[fd].object_id == CRUD_NO_OBJECT )  {
		
		// Create the object and store the bytes from the parameter buffer in it
		request = create_crud_request(0, CRUD_CREATE, count, 0, 0);
		response = crud_client_operation(request, tempBuf);
		
		// Check for CRUD command success
		extract_crud_response(response, &crud_file_table[fd].object_id, &req, &crud_file_table[fd].length, 
			&flag, &result);
		if( result )
			return -1; // ERROR - result code is 1 meaning there was a failure in command execution

		// Assign the position of the file to be at the end of the write
		crud_file_table[fd].position = count;
	}

	// CASE 1: The write DOES NOT change the size of the file
	else if( crud_file_table[fd].length >= count+crud_file_table[fd].position ) {
		
		// Allocate enough memory for tempBuf2 to read all of the contents of the file
		tempBuf2 = malloc(CRUD_MAX_OBJECT_SIZE);
		if( tempBuf2 == NULL )
			return -1; // ERROR - malloc returned a NULL pointer

		// Read the current file and store its contents into tempBuf2
		request = create_crud_request(crud_file_table[fd].object_id, CRUD_READ, CRUD_MAX_OBJECT_SIZE, 0, 0);
		response = crud_client_operation(request, tempBuf2);

		// Check for CRUD command success
		extract_crud_response(response, &crud_file_table[fd].object_id, &req, &crud_file_table[fd].length, 
			&flag, &result);
		if( result )
			return -1; // ERROR - result code is 1 meaning there was a failure in command execution

		// Starting at the current file position, copy 'count' bytes from buf into tempBuf2
		memcpy(&tempBuf2[crud_file_table[fd].position],buf,count);
		
		// Update the file using the newly crafted tempBuf2
		request = create_crud_request(crud_file_table[fd].object_id, CRUD_UPDATE, crud_file_table[fd].length, 0, 0);
		response = crud_client_operation(request, tempBuf2);

		// Check for CRUD command success
		extract_crud_response(response, &crud_file_table[fd].object_id, &req, &crud_file_table[fd].length, 
			&flag, &result);
		if( result )
			return -1; // ERROR - result code is 1 meaning there was a failure in command execution

		// Change position to the end of the write
		crud_file_table[fd].position += count;
	}

	// CASE 2: The write DOES change the size of the file
	else {
		// Determine the length that the new object will have
		newLength = count + crud_file_table[fd].position;
		// Allocate enough memory to hold the new object
		tempBuf2 = malloc( newLength );
		if( tempBuf2 == NULL )
			return -1; // ERROR - malloc returned a NULL pointer

		// Read the current file and store its contents into tempBuf2
		request = create_crud_request(crud_file_table[fd].object_id, CRUD_READ, newLength, 0, 0);
		response = crud_client_operation(request, tempBuf2);

		// Check for CRUD command success
		extract_crud_response(response, &crud_file_table[fd].object_id, &req, &crud_file_table[fd].length, 
			&flag, &result);
		if( result ) 
			return -1; // ERROR - result code is 1 meaning there was a failure in command execution
		
		// Starting at the file's current position, copy the bytes that need to be written into the buffer
		memcpy(&tempBuf2[crud_file_table[fd].position], buf, count);

		// Delete the old object of the shorter length
		request = create_crud_request(crud_file_table[fd].object_id, CRUD_DELETE, 0, 0, 0);
		response = crud_client_operation(request, NULL);

		// Check for CRUD command success
		extract_crud_response(response, &crud_file_table[fd].object_id, &req, &crud_file_table[fd].length, 
			&flag, &result);
		if( result )
			return -1; // ERROR - result code is 1 meaning there was a failure in command execution

		// Create the new object of the new longer length
		request = create_crud_request(0, CRUD_CREATE, newLength, 0, 0);
		response = crud_client_operation(request, tempBuf2);

		// Check for CRUD command success
		extract_crud_response(response, &crud_file_table[fd].object_id, &req, &crud_file_table[fd].length, 
			&flag, &result);
		if( result )
			return -1; // ERROR - result code is 1 meaning there was a failure in command execution

		// Assign the position of the file to be at the end of the write
		crud_file_table[fd].position += count;
	}

	// Free buffers if the buffers are not pointing to NULL and set them to NULL once freed
	if(tempBuf) {
		free(tempBuf);
		tempBuf = NULL;
	}
	if(tempBuf2) {
		free(tempBuf2);
		tempBuf2 = NULL;
	}

	return count;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_seek
// Description  : Seek to specific point in the file
//
// Inputs       : fd - the file descriptor for the file to seek
//                loc - offset from beginning of file to seek to
// Outputs      : 0 if successful or -1 if failure
// 
int32_t crud_seek(int16_t fd, uint32_t loc) {
	
	if( fd < 0 || fd > (CRUD_MAX_TOTAL_FILES-1) )
		return -1; // ERROR - requested file handle out of range
	
	if( !crud_file_table[fd].open )
		return -1; // ERROR - requested file has not yet been opened
	
	if( loc > crud_file_table[fd].length || loc < 0 ) 
		return -1; // ERROR - requested location out of range 

	// If both parameters are in range, change the current position in file to loc
	crud_file_table[fd].position = loc;
	return 0; // Success
}

// Module local methods

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crudIOUnitTest
// Description  : Perform a test of the CRUD IO implementation
//
// Inputs       : None
// Outputs      : 0 if successful or -1 if failure

int crudIOUnitTest(void) {

	// Local variables
	uint8_t ch;
	int16_t fh, i;
	int32_t cio_utest_length, cio_utest_position, count, bytes, expected;
	char *cio_utest_buffer, *tbuf;
	CRUD_UNIT_TEST_TYPE cmd;
	char lstr[1024];

	// Setup some operating buffers, zero out the mirrored file contents
	cio_utest_buffer = malloc(CRUD_MAX_OBJECT_SIZE);
	tbuf = malloc(CRUD_MAX_OBJECT_SIZE);
	memset(cio_utest_buffer, 0x0, CRUD_MAX_OBJECT_SIZE);
	cio_utest_length = 0;
	cio_utest_position = 0;

	// Format and mount the file system
	if (crud_format() || crud_mount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on format or mount operation.");
		return(-1);
	}

	// Start by opening a file
	fh = crud_open("temp_file.txt");
	if (fh == -1) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure open operation.");
		return(-1);
	}

	// Now do a bunch of operations
	for (i=0; i<CRUD_IO_UNIT_TEST_ITERATIONS; i++) {

		// Pick a random command
		if (cio_utest_length == 0) {
			cmd = CIO_UNIT_TEST_WRITE;
		} else {
			cmd = getRandomValue(CIO_UNIT_TEST_READ, CIO_UNIT_TEST_SEEK);
		}

		// Execute the command
		switch (cmd) {

		case CIO_UNIT_TEST_READ: // read a random set of data
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d at position %d", bytes, cio_utest_position);
			bytes = crud_read(fh, tbuf, count);
			if (bytes == -1) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Read failure.");
				return(-1);
			}

			// Compare to what we expected
			if (cio_utest_position+count > cio_utest_length) {
				expected = cio_utest_length-cio_utest_position;
			} else {
				expected = count;
			}
			if (bytes != expected) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : short/long read of [%d!=%d]", bytes, expected);
				return(-1);
			}
			if ( (bytes > 0) && (memcmp(&cio_utest_buffer[cio_utest_position], tbuf, bytes)) ) {

				bufToString((unsigned char *)tbuf, bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST R: %s", lstr);
				bufToString((unsigned char *)&cio_utest_buffer[cio_utest_position], bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST U: %s", lstr);

				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : read data mismatch (%d)", bytes);
				return(-1);
			}
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d match", bytes);


			// update the position pointer
			cio_utest_position += bytes;
			break;

		case CIO_UNIT_TEST_APPEND: // Append data onto the end of the file
			// Create random block, check to make sure that the write is not too large
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			if (cio_utest_length+count >= CRUD_MAX_OBJECT_SIZE) {

				// Log, seek to end of file, create random value
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : append of %d bytes [%x]", count, ch);
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", cio_utest_length);
				if (crud_seek(fh, cio_utest_length)) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", cio_utest_length);
					return(-1);
				}
				cio_utest_position = cio_utest_length;
				memset(&cio_utest_buffer[cio_utest_position], ch, count);

				// Now write
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes != count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : append failed [%d].", count);
					return(-1);
				}
				cio_utest_length = cio_utest_position += bytes;
			}
			break;

		case CIO_UNIT_TEST_WRITE: // Write random block to the file
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			// Check to make sure that the write is not too large
			if (cio_utest_length+count < CRUD_MAX_OBJECT_SIZE) {
				// Log the write, perform it
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : write of %d bytes [%x]", count, ch);
				memset(&cio_utest_buffer[cio_utest_position], ch, count);
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes!=count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : write failed [%d].", count);
					return(-1);
				}
				cio_utest_position += bytes;
				if (cio_utest_position > cio_utest_length) {
					cio_utest_length = cio_utest_position;
				}
			}
			break;

		case CIO_UNIT_TEST_SEEK:
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", count);
			if (crud_seek(fh, count)) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", count);
				return(-1);
			}
			cio_utest_position = count;
			break;

		default: // This should never happen
			CMPSC_ASSERT0(0, "CRUD_IO_UNIT_TEST : illegal test command.");
			break;

		}

#if DEEP_DEBUG
		// VALIDATION STEP: ENSURE OUR LOCAL IS LIKE OBJECT STORE
		CrudRequest request;
		CrudResponse response;
		CrudOID oid;
		CRUD_REQUEST_TYPES req;
		uint32_t length;
		uint8_t res, flags;

		// Make a fake request to get file handle, then check it
		request = construct_crud_request(crud_file_table[0].object_id, CRUD_READ, CRUD_MAX_OBJECT_SIZE, CRUD_NULL_FLAG, 0);
		response = crud_client_operation(request, tbuf);
		if ((deconstruct_crud_request(response, &oid, &req, &length, &flags, &res) != 0) || (res != 0))  {
			logMessage(LOG_ERROR_LEVEL, "Read failure, bad CRUD response [%x]", response);
			return(-1);
		}
		if ( (cio_utest_length != length) || (memcmp(cio_utest_buffer, tbuf, length)) ) {
			logMessage(LOG_ERROR_LEVEL, "Buffer/Object cross validation failed [%x]", response);
			bufToString((unsigned char *)tbuf, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VR: %s", lstr);
			bufToString((unsigned char *)cio_utest_buffer, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VU: %s", lstr);
			return(-1);
		}

		// Print out the buffer
		bufToString((unsigned char *)cio_utest_buffer, cio_utest_length, (unsigned char *)lstr, 1024 );
		logMessage(LOG_INFO_LEVEL, "CIO_UTEST: %s", lstr);
#endif

	}

	// Close the files and cleanup buffers, assert on failure
	if (crud_close(fh)) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure read comparison block.", fh);
		return(-1);
	}
	free(cio_utest_buffer);
	free(tbuf);

	// Format and mount the file system
	if (crud_unmount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on unmount operation.");
		return(-1);
	}

	// Return successfully
	return(0);
}







