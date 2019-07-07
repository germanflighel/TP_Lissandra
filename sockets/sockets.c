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
	entrada[strlen(entrada) - 1] = '\0';
	return entrada;
}

void separarEntrada(char* entrada, int* header, char** parametros) {
	char** entradaSeparada = string_n_split(entrada, 2, " ");

	*header = strToHeader(entradaSeparada[0]);

	free(entradaSeparada[0]);

	*parametros = entradaSeparada[1];

	free(entradaSeparada);
}

char* serializarDescribe(t_describe *package) {

	char *serializedPackage = malloc(
			sizeof(package->cant_tablas)
					+ package->cant_tablas * sizeof(t_metadata));

	int offset = 0;
	int size_to_send;

	size_to_send = sizeof(package->cant_tablas);
	memcpy(serializedPackage + offset, &(package->cant_tablas), size_to_send);
	offset += size_to_send;

	for (int x = 0; x < package->cant_tablas; x++) {
		size_to_send = sizeof(t_metadata);
		memcpy(serializedPackage + offset, &(package->tablas[x]), size_to_send);
		offset += size_to_send;

	}

	return serializedPackage;
}

char* serializarMensaje(char* mensaje, int* size) {

	uint32_t msg_length = strlen(mensaje);
	int total_size = sizeof(uint32_t) + msg_length;
	char *serializedPackage = malloc(total_size);

	memcpy(size, &total_size, sizeof(int));

	int offset = 0;
	int size_to_send;

	size_to_send = sizeof(msg_length);
	memcpy(serializedPackage + offset, &msg_length, size_to_send);
	offset += size_to_send;

	size_to_send = msg_length;
	memcpy(serializedPackage + offset, mensaje, size_to_send);

	return serializedPackage;
}

char* recieve_and_deserialize_mensaje(int socketCliente) {

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));
	char* mensaje;

	int msglength;
	status = recv(socketCliente, buffer, buffer_size, 0);
	memcpy(&(msglength), buffer, buffer_size);
	if (!status)
		return 0;

	//printf("%d \n", msglength);

	mensaje = malloc(msglength + 1);
	status = recv(socketCliente, mensaje, msglength, 0);
	if (!status)
		return 0;

	mensaje[msglength] = '\0';

	free(buffer);

	return mensaje;
}

int recieve_and_deserialize_describe(t_describe *package, int socketCliente) {

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint16_t));

	status = recv(socketCliente, buffer, sizeof(package->cant_tablas), 0);
	memcpy(&(package->cant_tablas), buffer, buffer_size);
	if (!status)
		return 0;

	int tamanio_lista = package->cant_tablas * sizeof(t_metadata);
	package->tablas = malloc(tamanio_lista);

	status = recv(socketCliente, package->tablas, tamanio_lista, 0);
	if (!status)
		return 0;

	free(buffer);

	return status;
}

int recieve_and_send_describe(t_describe *package, int socketCliente,
		int socketDestino) {

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint16_t));

	status = recv(socketCliente, buffer, sizeof(package->cant_tablas), 0);
	memcpy(&(package->cant_tablas), buffer, buffer_size);
	if (!status)
		return 0;

	int tamanio_lista = package->cant_tablas * sizeof(t_metadata);
	package->tablas = malloc(tamanio_lista);

	status = recv(socketCliente, package->tablas, tamanio_lista, 0);
	if (!status)
		return 0;

	free(buffer);

	return status;
}

int validarParametros(int header, char* parametros) {
	char** parametrosSeparados;
	if (parametros == NULL && header != DESCRIBE && header != JOURNAL) {
		return 0;
	}
	switch (header) {
	case JOURNAL:
		if (parametros != NULL) {
			return 0;
		}
		break;
	case SELECT:
		parametrosSeparados = string_split(parametros, " ");

		if (cant_parametros(parametrosSeparados) != 2) {
			string_iterate_lines(parametrosSeparados, (void*) free);
			free(parametrosSeparados);
			return 0;
		} else if (!esUnNumero(parametrosSeparados[1])) {
			string_iterate_lines(parametrosSeparados, (void*) free);
			free(parametrosSeparados);
			return 0;
		}
		string_iterate_lines(parametrosSeparados, (void*) free);
		free(parametrosSeparados);
		break;
	case INSERT:

		parametrosSeparados = string_n_split(parametros, 3, " ");

		if (cant_parametros(parametrosSeparados) < 3
				|| !(strlen(parametrosSeparados[2]) >= 3)
				|| parametrosSeparados[2][0] != '"'
				|| parametrosSeparados[2][strlen(parametrosSeparados[2]) - 1]
						!= '"') {
			string_iterate_lines(parametrosSeparados, (void*) free);
			free(parametrosSeparados);
			return 0;
		} else if (!esUnNumero(parametrosSeparados[1])) {
			string_iterate_lines(parametrosSeparados, (void*) free);
			free(parametrosSeparados);
			;
			return 0;
		}
		string_iterate_lines(parametrosSeparados, (void*) free);
		free(parametrosSeparados);
		break;
	case CREATE:
		parametrosSeparados = string_split(parametros, " ");
		if (cant_parametros(parametrosSeparados) != 4) {
			string_iterate_lines(parametrosSeparados, (void*) free);
			free(parametrosSeparados);
			return 0;
		} else if (!esUnNumero(parametrosSeparados[3])
				|| !esUnNumero(parametrosSeparados[2])) {
			string_iterate_lines(parametrosSeparados, (void*) free);
			free(parametrosSeparados);
			return 0;
		}
		string_iterate_lines(parametrosSeparados, (void*) free);
		free(parametrosSeparados);
		break;
	case RUN:
		parametrosSeparados = string_split(parametros, " ");
		if (cant_parametros(parametrosSeparados) != 1) {
			string_iterate_lines(parametrosSeparados, (void*) free);
			free(parametrosSeparados);
			return 0;
		}
		string_iterate_lines(parametrosSeparados, (void*) free);
		free(parametrosSeparados);
		break;
	case DESCRIBE:
		if (parametros == NULL) {
			return 1;
		}
		parametrosSeparados = string_split(parametros, " ");
		if (cant_parametros(parametrosSeparados) != 1) {
			string_iterate_lines(parametrosSeparados, (void*) free);
			free(parametrosSeparados);
			return 0;
		}
		string_iterate_lines(parametrosSeparados, (void*) free);
		free(parametrosSeparados);
		break;
	}
	return 1;
}

int esUnNumero(char* parametro) {
	int num;
	num = atoi(parametro);

	return !(num == 0 && parametro[0] != '0');
}

int fill_package_select(t_PackageSelect *package, char* parametros) {

	if (!validarParametros(SELECT, parametros)) {
		return 0;
	}

	char** parametrosSeparados = string_split(parametros, " ");

	package->header = SELECT;
	package->tabla_long = strlen(parametrosSeparados[0]);
	package->tabla = malloc(package->tabla_long + 1);

	memcpy(package->tabla, parametrosSeparados[0], package->tabla_long + 1);

	package->key = atoi(parametrosSeparados[1]);

	package->total_size = sizeof(package->header) + sizeof(package->tabla_long)
			+ sizeof(package->key) + package->tabla_long;

	string_iterate_lines(parametrosSeparados, (void*) free);
	free(parametrosSeparados);
	return 1;
}

int fill_package_create(t_PackageCreate* package, char* parametros) {

	if (!validarParametros(CREATE, parametros)) {
		return 0;
	}

	char** parametrosSeparados = string_split(parametros, " ");

	package->header = CREATE;
	package->tabla_long = strlen(parametrosSeparados[0]);
	package->tabla = malloc(package->tabla_long + 1);

	string_to_upper(parametrosSeparados[0]);

	memcpy(package->tabla, parametrosSeparados[0], package->tabla_long + 1);

	package->consistency = consistency_to_int(parametrosSeparados[1]);

	if (!package->consistency)
		return 0;

	package->partitions = atoi(parametrosSeparados[2]);

	package->compaction_time = atol(parametrosSeparados[3]);

	package->total_size = sizeof(package->header) + sizeof(package->tabla_long)
			+ sizeof(package->consistency) + sizeof(package->compaction_time)
			+ sizeof(package->partitions) + package->tabla_long;

	string_iterate_lines(parametrosSeparados, (void*) free);
	free(parametrosSeparados);
	return 1;
}

int fill_package_describe(t_PackageDescribe* package, char* parametros) {

	if (parametros == NULL) {
		printf("Aca\n");
		package->tabla_long = 0;
		package->nombre_tabla = NULL;

		package->total_size = sizeof(package->header)
				+ sizeof(package->tabla_long) + package->tabla_long;
		return 1;
	}

	char** parametrosSeparados = string_split(parametros, " ");

	if (cant_parametros(parametrosSeparados) != 1) {
		return 0;
	}

	package->header = DESCRIBE;
	package->tabla_long = strlen(parametrosSeparados[0]);
	package->nombre_tabla = malloc(package->tabla_long + 1);
	memcpy(package->nombre_tabla, parametrosSeparados[0],
			package->tabla_long + 1);

	package->total_size = sizeof(package->header) + sizeof(package->tabla_long)
			+ package->tabla_long;

	string_iterate_lines(parametrosSeparados, (void*) free);
	free(parametrosSeparados);

	return 1;
}

int fill_package_drop(t_PackageDrop* package, char* parametros) {

	char** parametrosSeparados = string_split(parametros, " ");

	if (cant_parametros(parametrosSeparados) != 1) {
		return 0;
	}

	package->header = DROP;
	package->tabla_long = strlen(parametrosSeparados[0]);
	package->nombre_tabla = malloc(package->tabla_long + 1);
	strcpy(package->nombre_tabla, parametrosSeparados[0]);

	package->total_size = sizeof(package->header) + sizeof(package->tabla_long)
			+ package->tabla_long;
	return 1;
}

char* serializarRequestDescribe(t_PackageDescribe* package) {

	if (!package->tabla_long) {
		package->tabla_long = 1;
		package->nombre_tabla = " ";
		//strcpy(package->nombre_tabla, " ");
	}

	int offset = 0;
	int size_to_send;
	int header = DESCRIBE;
	char* serializedPackage;

	int tablalen = strlen(package->nombre_tabla);
	int size_total = sizeof(int) + sizeof(int) + tablalen;

	serializedPackage = malloc(size_total);

	size_to_send = sizeof(int);
	memcpy(serializedPackage + offset, &header, size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(package->tabla_long);
	memcpy(serializedPackage + offset, &package->tabla_long, size_to_send);
	offset += size_to_send;

	size_to_send = package->tabla_long;
	memcpy(serializedPackage + offset, package->nombre_tabla, size_to_send);

	package->total_size = sizeof(package->header) + sizeof(package->tabla_long)
			+ package->tabla_long;

	return serializedPackage;
}

int recieve_and_deserialize_describe_request(t_PackageDescribe *package,
		int socketCliente) {

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));

	status = recv(socketCliente, buffer, sizeof(package->tabla_long), 0);
	memcpy(&(package->tabla_long), buffer, buffer_size);
	if (!status)
		return 0;

	package->nombre_tabla = malloc(package->tabla_long + 1);

	status = recv(socketCliente, package->nombre_tabla, package->tabla_long, 0);
	if (!status)
		return 0;

	package->nombre_tabla[package->tabla_long] = '\0';

	free(buffer);

	package->total_size = sizeof(package->header) + sizeof(package->tabla_long)
			+ package->tabla_long;

	return status;
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

char* serializarDrop(t_PackageDrop *package) {

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
	memcpy(serializedPackage + offset, package->nombre_tabla, size_to_send);

	return serializedPackage;
}

char* serializarCreate(t_PackageCreate *package) {

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

	size_to_send = sizeof(package->consistency);
	memcpy(serializedPackage + offset, &(package->consistency), size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(package->compaction_time);
	memcpy(serializedPackage + offset, &(package->compaction_time),
			size_to_send);
	offset += size_to_send;
	size_to_send = sizeof(package->partitions);
	memcpy(serializedPackage + offset, &(package->partitions), size_to_send);

	return serializedPackage;
}

int parametros_insert_filesystem(char* parametros, t_PackageInsert *package) {
	char** parametrosSeparadosPorComilla = string_split(parametros, "\"");

	if (cant_parametros(parametrosSeparadosPorComilla) != 3) {
		return 0;
	}

	char* tablaKey = parametrosSeparadosPorComilla[0];
	char* timeStamp = parametrosSeparadosPorComilla[2];
	string_trim(&tablaKey);
	string_trim(&timeStamp);

	char** tablakeySeparada = string_split(tablaKey, " ");
	if (cant_parametros(tablakeySeparada) != 2
			|| string_contains(timeStamp, " ")) {
		return 0;
	}

	package->tabla_long = strlen(tablakeySeparada[0]);
	package->tabla = malloc(strlen(tablakeySeparada[0]));
	strcpy(package->tabla, tablakeySeparada[0]);

	package->key = atoi(tablakeySeparada[1]);

	package->timestamp = atoi(timeStamp);

	package->value_long = strlen(parametrosSeparadosPorComilla[1]);

	package->value = malloc(package->value_long + 1);
	strcpy(package->value, parametrosSeparadosPorComilla[1]);

	return 2;
}

int fill_package_insert(t_PackageInsert *package, char* parametros, int filesys) {

	if (!validarParametros(INSERT, parametros)) {
		return 0;
	}

	char** parametrosSeparados = string_n_split(parametros, 3, " ");

	int cantParametros = cant_parametros(parametrosSeparados);

	int fs = 1;

	if (!(strlen(parametrosSeparados[2]) >= 3)
			|| parametrosSeparados[2][0] != '"'
			|| parametrosSeparados[2][strlen(parametrosSeparados[2]) - 1]
					!= '"') {
		if (filesys) {
			fs = parametros_insert_filesystem(parametros, package);
		} else {
			return 0;
		}
	}

	if (!fs) {
		return 0;
	}

	package->header = INSERT;
	if (fs != 2) {
		package->tabla_long = strlen(parametrosSeparados[0]);
		package->tabla = malloc(package->tabla_long + 1);

		memcpy(package->tabla, parametrosSeparados[0], package->tabla_long + 1);

		package->value_long = strlen(parametrosSeparados[2]) - 2;

		package->value = malloc(package->value_long + 1);
		memcpy(package->value, parametrosSeparados[2] + 1, package->value_long);
		package->value[package->value_long] = '\0';

		package->key = atoi(parametrosSeparados[1]);

		package->timestamp = (unsigned) time(NULL);
	}

	package->total_size = sizeof(package->header) + sizeof(package->value_long)
			+ sizeof(package->tabla_long) + sizeof(package->key)
			+ sizeof(package->timestamp) + package->value_long
			+ package->tabla_long;

	string_iterate_lines(parametrosSeparados, (void*) free);
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
	int buffer_size = sizeof(uint32_t);
	char *buffer = malloc(buffer_size);

	status = recv(socketCliente, buffer, buffer_size, 0);

	memcpy(&(header), buffer, buffer_size);

	if (!status) {
		free(buffer);
		return 0;
	}

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

	package->tabla = malloc(package->tabla_long + 1);

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

	package->total_size = sizeof(package->header) + sizeof(package->key)
			+ sizeof(package->tabla_long) + package->tabla_long;

	return status;
}

int recieve_and_deserialize_create(t_PackageCreate *package, int socketCliente) {

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));

	status = recv(socketCliente, buffer, sizeof(package->tabla_long), 0);
	memcpy(&(package->tabla_long), buffer, buffer_size);
	if (!status)
		return 0;

	package->tabla = malloc(package->tabla_long + 1);

	status = recv(socketCliente, package->tabla, package->tabla_long, 0);
	if (!status)
		return 0;

	package->tabla[package->tabla_long] = '\0';

	status = recv(socketCliente, buffer, sizeof(package->consistency), 0);
	memcpy(&(package->consistency), buffer, buffer_size);
	if (!status)
		return 0;

	status = recv(socketCliente, buffer, sizeof(package->compaction_time), 0);
	memcpy(&(package->compaction_time), buffer, buffer_size);
	if (!status)
		return 0;

	status = recv(socketCliente, buffer, sizeof(package->partitions), 0);
	memcpy(&(package->partitions), buffer, buffer_size);
	if (!status)
		return 0;

	free(buffer);

	package->total_size = sizeof(package->header) + sizeof(package->tabla_long)
			+ sizeof(package->consistency) + sizeof(package->compaction_time)
			+ sizeof(package->partitions) + package->tabla_long;

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

	package->tabla = malloc(package->tabla_long + 1);

	status = recv(socketCliente, package->tabla, package->tabla_long, 0);
	if (!status)
		return 0;

	package->tabla[package->tabla_long] = '\0';

	status = recv(socketCliente, buffer, sizeof(package->value_long), 0);
	memcpy(&(package->value_long), buffer, buffer_size);
	if (!status)
		return 0;

	package->value = malloc(package->value_long + 1);

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

	package->total_size = sizeof(package->header) + sizeof(package->value_long)
			+ sizeof(package->tabla_long) + sizeof(package->key)
			+ sizeof(package->timestamp) + package->value_long
			+ package->tabla_long;

	return status;
}

int recieve_and_deserialize_drop(t_PackageDrop* package, int socketCliente) {

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));

	status = recv(socketCliente, buffer, sizeof(package->tabla_long), 0);
	memcpy(&(package->tabla_long), buffer, buffer_size);
	if (!status)
		return 0;

	package->nombre_tabla = malloc(package->tabla_long + 1);

	status = recv(socketCliente, package->nombre_tabla, package->tabla_long, 0);
	if (!status)
		return 0;

	package->nombre_tabla[package->tabla_long] = '\0';

	free(buffer);

	package->total_size = sizeof(package->header) + sizeof(package->tabla_long)
			+ package->tabla_long;

	return status;
}

char* serializarRespuestaSelect(t_Respuesta_Select *package) {

	char *serializedPackage = malloc(
			sizeof(package->result) + sizeof(package->value_long)
					+ package->value_long + sizeof(package->timestamp));

	int offset = 0;
	int size_to_send;

	size_to_send = sizeof(package->result);
	memcpy(serializedPackage + offset, &(package->result), size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(package->timestamp);
	memcpy(serializedPackage + offset, &(package->timestamp), size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(package->value_long);
	memcpy(serializedPackage + offset, &(package->value_long), size_to_send);
	offset += size_to_send;

	size_to_send = package->value_long;
	memcpy(serializedPackage + offset, package->value, size_to_send);

	return serializedPackage;
}

int recieve_and_deserialize_RespuestaSelect(t_Respuesta_Select *package,
		int socketServidor) {

	int status;
	int buffer_size;
	char *buffer = malloc(buffer_size = sizeof(uint32_t));

	status = recv(socketServidor, buffer, sizeof(package->result), 0);
	memcpy(&(package->result), buffer, buffer_size);
	if (!status)
		return 0;

	status = recv(socketServidor, buffer, sizeof(package->timestamp), 0);
	memcpy(&(package->timestamp), buffer, buffer_size);
	if (!status)
		return 0;

	status = recv(socketServidor, buffer, sizeof(package->value_long), 0);
	memcpy(&(package->value_long), buffer, buffer_size);
	if (!status)
		return 0;

	package->value = malloc(package->value_long + 1);

	status = recv(socketServidor, package->value, package->value_long, 0);
	if (!status)
		return 0;

	package->value[package->value_long] = '\0';

	free(buffer);

	return status;
}

int cant_parametros(char** params) {
	int i = 0;
	while (params[i] != NULL) {
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

void enviar_describe(char* tabla, int socket) {

	int offset = 0;
	int size_to_send;
	int header = DESCRIBE;
	char* serializedPackage;
	int tabla_len = strlen(tabla);

	if (tabla == NULL) {
		tabla_len = 1;
	}

	int tablalen = strlen(tabla);
	int size_total = sizeof(int) + sizeof(int) + tabla_len;

	serializedPackage = malloc(size_total);

	size_to_send = sizeof(int);
	memcpy(serializedPackage + offset, &size_total, size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(int);
	memcpy(serializedPackage + offset, &header, size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(int);
	memcpy(serializedPackage + offset, &header, size_to_send);
	offset += size_to_send;

	size_to_send = sizeof(int);
	memcpy(serializedPackage + offset, &tablalen, size_to_send);
	offset += size_to_send;

	if (tabla != NULL) {
		size_to_send = strlen(tabla);
		memcpy(serializedPackage + offset, tabla, size_to_send);
	} else {
		size_to_send = 1;
		char* vacio = " ";
		memcpy(serializedPackage + offset, vacio, size_to_send);
	}

	send(socket, serializedPackage, sizeof(int) + sizeof(int) + strlen(tabla),
			0);

	dispose_package(&serializedPackage);
}

int recibir_handshake(int idEsperado, int socket) {

	t_Handshake package;
	package.header = recieve_header(socket);

	if (HANDSHAKE == package.header) {
		recieve_and_deserialize_handshake(&package, socket);
		if (package.id == idEsperado) {
			return 1;
		}
	}
	return 0;
}

int recibir_numero_memoria(int socket) {

	t_Handshake package;
	package.header = recieve_header(socket);

	if (HANDSHAKE == package.header) {
		recieve_and_deserialize_handshake(&package, socket);
		return package.id;
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

int consistency_to_int(char* consistency) {
	if (strcmp(consistency, "SC") == 0) {
		return SC;
	} else if (strcmp(consistency, "SHC") == 0) {
		return SHC;
	} else if (strcmp(consistency, "EC") == 0) {
		return EC;
	}
	return 0;
}

char* consistency_to_str(int consistency) {
	switch (consistency) {
	case SC:
		return "SC";
	case SHC:
		return "SHC";
	case EC:
		return "EC";
	}
}
