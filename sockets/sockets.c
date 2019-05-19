#include "sockets.h"

int strToHeader(char* header) {

	if (strcmp(header, "SELECT") == 0) {
		return SELECT;
	} else if (strcmp(header, "INSERT") == 0) {
		return INSERT;
	} else if (strcmp(header, "DESCRIBE") == 0) {
		return DESCRIBE;
	} else if (strcmp(header, "DROP") == 0) {
		return DROP;
	} else if (strcmp(header, "CREATE") == 0) {
		return CREATE;
	} else if (strcmp(header, "JOURNAL") == 0) {
		return JOURNAL;
	} else if (strcmp(header, "RUN") == 0) {
		return RUN;
	} else if (strcmp(header, "ADD") == 0) {
		return ADD;
	} else if (strcmp(header, "exit") == 0) {
		return EXIT_CONSOLE;
	}
	return ERROR;
}


char* leerConsola() {
	char* entrada = malloc(MAX_MESSAGE_SIZE);
	fgets(entrada, MAX_MESSAGE_SIZE, stdin);
	entrada[strlen(entrada)-1] = '\0';
	return entrada;
}


void separarEntrada(char* entrada, int* header, char** parametros) {
	char** entradaSeparada = string_n_split(entrada,2," ");

	*header = strToHeader(entradaSeparada[0]);
	*parametros = entradaSeparada[1];

	free(entradaSeparada);
}

int fill_package_select(t_PackageSelect *package ,char* parametros) {

	char** parametrosSeparados = string_split(parametros, " ");

	if(cant_parametros(parametrosSeparados) != 2){
		return 0;
	}


	package->header = SELECT;
	package->tabla_long = strlen(parametrosSeparados[0]);
	package->tabla = malloc(package->tabla_long+1);

	memcpy(package->tabla, parametrosSeparados[0], package->tabla_long+1);

	package->key = atoi(parametrosSeparados[1]);

	package->total_size = sizeof(package->header) + sizeof(package->tabla_long) + sizeof(package->key) + package->tabla_long;

	free(parametrosSeparados);
	return 1;
}


char* serializarSelect(t_PackageSelect *package) {

	char *serializedPackage = malloc(package->total_size);

	int offset = 0;
	int size_to_send;

	size_to_send = sizeof(package->header);
	memcpy(serializedPackage + offset, &(package->header), size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(package->tabla_long);
	memcpy(serializedPackage + offset, &(package->tabla_long), size_to_send);
	offset += size_to_send;

	size_to_send = package->tabla_long;
	memcpy(serializedPackage + offset, package->tabla, size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(package->key);
	memcpy(serializedPackage + offset, &(package->key), size_to_send);

	return serializedPackage;
}

int fill_package_insert(t_PackageInsert *package ,char* parametros, int filesys) {

	char** parametrosSeparados = string_split(parametros, " ");

	int cantParametros = cant_parametros(parametrosSeparados);

	if(cantParametros != 3){
		if(!filesys || cantParametros != 4){
			return 0;
		}
	}


	package->header = INSERT;
	package->tabla_long = strlen(parametrosSeparados[0]);
	package->tabla = malloc(package->tabla_long+1);

	memcpy(package->tabla, parametrosSeparados[0], package->tabla_long+1);

	package->value_long = strlen(parametrosSeparados[2]);
	package->value = malloc(package->value_long+1);

	memcpy(package->value, parametrosSeparados[2], package->value_long+1);

	package->key = atoi(parametrosSeparados[1]);

	if(filesys && cantParametros == 4){
		package->timestamp = atoi(parametrosSeparados[3]);
	}else{
		package->timestamp = (unsigned)time(NULL);
	}


	package->total_size = sizeof(package->header)+ sizeof(package->value_long) + sizeof(package->tabla_long) + sizeof(package->key)+ sizeof(package->timestamp)+ package->value_long + package->tabla_long;

	free(parametrosSeparados);
	return 1;
}

char* serializarInsert(t_PackageInsert *package) {

	char *serializedPackage = malloc(package->total_size);

	int offset = 0;
	int size_to_send;

	size_to_send = sizeof(package->header);
	memcpy(serializedPackage + offset, &(package->header), size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(package->tabla_long);
	memcpy(serializedPackage + offset, &(package->tabla_long), size_to_send);
	offset += size_to_send;

	size_to_send = package->tabla_long;
	memcpy(serializedPackage + offset, package->tabla, size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(package->value_long);
	memcpy(serializedPackage + offset, &(package->value_long), size_to_send);
	offset += size_to_send;

	size_to_send = package->value_long;
	memcpy(serializedPackage + offset, package->value, size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(package->timestamp);
	memcpy(serializedPackage + offset, &(package->timestamp), size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(package->key);
	memcpy(serializedPackage + offset, &(package->key), size_to_send);

	return serializedPackage;
}

int recieve_header(int socketCliente) {

	uint32_t header;

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));

	status = recv(socketCliente, buffer, buffer_size, 0);

	memcpy(&(header), buffer, buffer_size);

	if (!status)
		return 0;
	free(buffer);
	return header;
}

int recieve_and_deserialize_select(t_PackageSelect *package, int socketCliente) {

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));

	status = recv(socketCliente, buffer, sizeof(package->tabla_long), 0);
	memcpy(&(package->tabla_long), buffer, buffer_size);
	if (!status)
		return 0;

//	printf("Tabla_long: %d \n", tabla_long);

	package->tabla = malloc(package->tabla_long);

	status = recv(socketCliente, package->tabla, package->tabla_long, 0);
	if (!status)
		return 0;

	package->tabla[package->tabla_long] = '\0';

//	printf("Tabla: %s \n", package->tabla);

		status = recv(socketCliente, buffer, sizeof(package->key), 0);
		memcpy(&(package->key), buffer, buffer_size);
		if (!status)
			return 0;

//	printf("Key: %d \n", package->key);

	if (!status)
		return 0;

	free(buffer);

	package->total_size = sizeof(package->header) + sizeof(package->key) +sizeof(package->tabla_long) + package->tabla_long;

	return status;
}

int recieve_and_deserialize_insert(t_PackageInsert *package, int socketCliente) {

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));

	status = recv(socketCliente, buffer, sizeof(package->tabla_long), 0);
	memcpy(&(package->tabla_long), buffer, buffer_size);
	if (!status)
		return 0;


	package->tabla = malloc(package->tabla_long);

	status = recv(socketCliente, package->tabla, package->tabla_long, 0);
	if (!status)
		return 0;

	package->tabla[package->tabla_long] = '\0';

	status = recv(socketCliente, buffer, sizeof(package->value_long), 0);
		memcpy(&(package->value_long), buffer, buffer_size);
		if (!status)
			return 0;


		package->value = malloc(package->value_long);

		status = recv(socketCliente, package->value, package->value_long, 0);
		if (!status)
			return 0;

		package->value[package->value_long] = '\0';

		status = recv(socketCliente, buffer, sizeof(package->timestamp), 0);
				memcpy(&(package->timestamp), buffer, buffer_size);
				if (!status)
					return 0;

				status = recv(socketCliente, buffer, sizeof(package->key), 0);
				memcpy(&(package->key), buffer, buffer_size);
				if (!status)
					return 0;


	if (!status)
		return 0;

	free(buffer);

	package->total_size = sizeof(package->header)+ sizeof(package->value_long) + sizeof(package->tabla_long) + sizeof(package->key)+ sizeof(package->timestamp)+ package->value_long + package->tabla_long;

	return status;
}



int cant_parametros(char** params){
	int i = 0;
	while(params[i]!= NULL){
		i++;
	}
	return i;
}


void enviar_handshake(int id, int socket) {

	t_Handshake package;
	package.header = HANDSHAKE;
	package.id = id;

	char* serializedHandshake = serializarHandShake(&package);
	send(socket, serializedHandshake,
			sizeof(package.header) + sizeof(package.id), 0);

	dispose_package(&serializedHandshake);
}

int recibir_handshake(int idEsperado, int socket) {

	t_Handshake package;
	package.header = recieve_header(socket);

	if (HANDSHAKE == package.header) {
		printf("%d \n",package.header);
		recieve_and_deserialize_handshake(&package, socket);
		if (package.id == idEsperado) {
			return 1;
		}
	}
	return 0;
}

char* serializarHandShake(t_Handshake *package) {

	char *serializedPackage = malloc(
			sizeof(package->header) + sizeof(package->id));

	int offset = 0;
	int size_to_send;

	size_to_send = sizeof(package->header);

	memcpy(serializedPackage + offset, &(package->header), size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(package->id);
	memcpy(serializedPackage + offset, &(package->id), size_to_send);

	return serializedPackage;

}





int recieve_and_deserialize_handshake(t_Handshake *package, int socketCliente) {

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(package->id));

	uint32_t id;
	status = recv(socketCliente, buffer, buffer_size, 0);
	memcpy(&(id), buffer, buffer_size);
	if (!status)
		return 0;

	package->id = id;

	free(buffer);

	return status;
}

void dispose_package(char **package) {
	free(*package);
}

int recieve_and_deserialize(t_PackagePosta *package, int socketCliente) {

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));

	uint32_t header;
	status = recv(socketCliente, buffer, sizeof(package->header), 0);
	memcpy(&(header), buffer, buffer_size);
	if (!status)
		return 0;

	package->header = header;

	uint32_t message_long;
	status = recv(socketCliente, buffer, sizeof(package->message_long), 0);
	memcpy(&(message_long), buffer, buffer_size);
	if (!status)
		return 0;

	package->message = malloc(message_long + 1);

	status = recv(socketCliente, package->message, message_long, 0);
	if (!status)
		return 0;

	free(buffer);

	package->message_long = message_long;
	package->total_size = buffer_size * 2 + message_long;

	return status;
}

char* serializarDescribe(t_describe *package) {

    char serializedPackage = malloc(sizeof(package->cant_tablas)+package->cant_tablas*sizeof(t_metadata));

    int offset = 0;
    int size_to_send;

    size_to_send = sizeof(package->cant_tablas);
    memcpy(serializedPackage + offset, &(package->cant_tablas), size_to_send);
    offset += size_to_send;

    for(int x=0;x<package->cant_tablas;x++)
    {
        size_to_send = sizeof(t_metadata);
        memcpy(serializedPackage + offset, &(package->tablas[x]), size_to_send);
        offset += size_to_send;

    }

    return serializedPackage;
}

int recieve_and_deserialize_describe(t_describe *package, int socketCliente) {

    int status;
    int buffer_size;
    char buffer = malloc(buffer_size = sizeof(uint16_t));

    status = recv(socketCliente, buffer, sizeof(package->cant_tablas), 0);
    memcpy(&(package->cant_tablas), buffer, buffer_size);
    if (!status)
        return 0;

    int tamanio_lista = package->cant_tablas*sizeof(t_metadata);
    package->tablas = malloc(tamanio_lista);

    status = recv(socketCliente, package->tablas, tamanio_lista, 0);
    if (!status)
        return 0;

    free(buffer);

    return status;
}


/*
 void fill_package(t_PackagePosta *package) {

	char* entrada = malloc(MAX_MESSAGE_SIZE);
	fgets(entrada, MAX_MESSAGE_SIZE, stdin);

	if (strcmp(entrada, "exit\n") == 0) {
		package->header = -1;
		free(entrada);
		return;
	}

	char** entradaSeparada = string_split(entrada, " ");

	if (entradaSeparada[1] == NULL) {
		entradaSeparada[0][strlen(entradaSeparada[0]) - 1] = '\0';
	}

	package->header = strToHeader(entradaSeparada[0]);

	if (package->header == ERROR) {
		free(entrada);
		free(entradaSeparada);
		return;
	}

	char* sinHeader;

	if (entradaSeparada[1] == NULL) { // Manejamos si entra una sola parabra sin parametros
		(package->message)[0] = '\0';
		package->message_long = 1;	//Solo el fin de caracter
		package->total_size = sizeof(package->message_long)
				+ package->message_long + sizeof(package->header);
		return;
	}

	sinHeader = string_substring_from(entrada, strlen(entradaSeparada[0]) + 1);

	char* timestamp = string_itoa((unsigned) time(NULL));

	free(entrada);
	free(entradaSeparada);

	if (package->header == INSERT) {
		memcpy(package->message, sinHeader, strlen(sinHeader) - 1);
		memcpy((package->message + strlen(sinHeader)), timestamp,
				strlen(timestamp));
		(package->message)[strlen(sinHeader) - 1] = ' ';
		(package->message)[strlen(sinHeader) + strlen(timestamp)] = '\0';
	} else {
		memcpy(package->message, sinHeader, strlen(sinHeader) - 1);
		(package->message)[strlen(sinHeader) - 1] = '\0';
	}

	free(timestamp);
	free(sinHeader);
	package->message_long = strlen(package->message) + 1;// Me guardo lugar para el \0

	package->total_size = sizeof(package->message_long) + package->message_long
			+ sizeof(package->header);
}



char* serializarOperandos(t_PackagePosta *package) {

	char *serializedPackage = malloc(package->total_size);

	int offset = 0;
	int size_to_send;

	size_to_send = sizeof(package->header);
	memcpy(serializedPackage + offset, &(package->header), size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(package->message_long);
	memcpy(serializedPackage + offset, &(package->message_long), size_to_send);
	offset += size_to_send;

	size_to_send = package->message_long;
	memcpy(serializedPackage + offset, package->message, size_to_send);

	return serializedPackage;
}

*/


