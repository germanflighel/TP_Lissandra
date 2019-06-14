/*
 ============================================================================
 Name        : Memory.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "Memory.h"

void *inputFunc(void *);
//pthread_mutex_t lock;

//int es_primera_memoria;

int numero_memoria;

//Estructura Memoria Principal
int cant_paginas;
void* memoriaPrincipal;
t_list* tabla_segmentos;
Tabla_paginas tabla_paginas;
char* bit_map;
int max_value_size;
double memory_size;
char* conf_path;
//Estructura Memoria Principal

char* puerto_propio;
int socketCliente;

int retardo_mem;
int retardo_fs;

int main() {

	//Estructura Conexiones
	struct addrinfo hints;
	struct addrinfo *serverInfo;
	char* ip;
	char* puerto;
	//Estructura Conexiones

	//Estructura Conexiones
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC; // Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP
	//Estructura Conexiones

	//Cofig Path
	printf("Ingrese nombre del archivo de configuración \n");
	char* entrada = leerConsola();
	conf_path = malloc(strlen(entrada));
	strcpy(conf_path, entrada);
	free(entrada);

	//Cofig Path

	//Abro log y config
	abrir_log();

	t_config *conection_conf;
	abrir_con(&conection_conf);
	printf("Archivo de configuración \"%s\" levantado\n", conf_path);
	//Abro log y config

	memory_size = config_get_double_value(conection_conf, "TAM_MEM");
	//es_primera_memoria = config_get_int_value(conection_conf, "START_UP_MEM");
	numero_memoria = config_get_int_value(conection_conf, "MEMORY_NUMBER");

	retardo_mem = 1000*config_get_int_value(conection_conf, "RETARDO_MEM");
	retardo_fs = 1000*config_get_int_value(conection_conf, "RETARDO_FS");

	ip = config_get_string_value(conection_conf, "IP");
	puerto = config_get_string_value(conection_conf, "PUERTO_FS");
	puerto_propio = config_get_string_value(conection_conf, "PUERTO");

	log_info(g_logger, puerto_propio);

	getaddrinfo(ip, puerto, &hints, &serverInfo);// Carga en serverInfo los datos de la conexion

	//Cierro config

	//Socket a lfs
	int lfsSocket;
	lfsSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol);

	connect(lfsSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);

	freeaddrinfo(serverInfo);

	log_debug(g_logger, "Conectado al LFS");
	//Socket a lfs

	//Handshake con LFS
	enviar_handshake(MEMORY, lfsSocket);

	recv(lfsSocket, &max_value_size, sizeof(u_int16_t), 0);
	log_info(g_logger, "Maximo tamaño del value: %d.", max_value_size);
	char* loguear = string_itoa(max_value_size);
	log_info(g_logger, loguear);
	free(loguear);

	log_info(g_logger, puerto_propio);

	/*
	 t_describe describeRecibido;
	 if (es_primera_memoria) {

	 recieve_and_deserialize_describe(&describeRecibido, lfsSocket);

	 }
	 log_info(g_logger, puerto_propio);
	 */

	//Handshake con LFS
	//inicializacion memoria
	//recibir_max_value_size(lfsSocket,&max_value_size);
	typedef struct Pagina {
		long timeStamp;
		uint16_t key;
		char value[max_value_size];
	} Pagina;

	cant_paginas = floor(memory_size / (double) sizeof(Pagina));
	//printf("%d \n", cant_paginas);
	memoriaPrincipal = malloc(memory_size);
	tabla_segmentos = list_create();
	tabla_paginas.renglones = malloc(cant_paginas * sizeof(Renglon_pagina));

	bit_map = malloc(cant_paginas);
	for (int pag = 0; pag < cant_paginas; pag++) {
		bit_map[pag] = 0;
	}

	//inicializacion memoria

	//Socket a kernel (listener)
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;		// No importa si uso IPv4 o IPv6
	hints.ai_flags = AI_PASSIVE;// Asigna el address del localhost: 127.0.0.1
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	getaddrinfo(NULL, puerto_propio, &hints, &serverInfo); // Notar que le pasamos NULL como IP, ya que le indicamos que use localhost en AI_PASSIVE

	config_destroy(conection_conf);

	int listenningSocket;
	listenningSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol);

	int waiting = 1;
	if (bind(listenningSocket, serverInfo->ai_addr, serverInfo->ai_addrlen)
			!= -1) {
		waiting = 0;
	}
	while (waiting) {
		if (bind(listenningSocket, serverInfo->ai_addr, serverInfo->ai_addrlen)
				!= -1) {
			waiting = 0;
		}
		sleep(5);
	}

	freeaddrinfo(serverInfo); // Ya no lo vamos a necesitar

	printf("Esperando kernel... \n");

	listen(listenningSocket, BACKLOG); // IMPORTANTE: listen() es una syscall BLOQUEANTE.

	struct sockaddr_in addr; // Esta estructura contendra los datos de la conexion del cliente. IP, puerto, etc.
	socklen_t addrlen = sizeof(addr);

	socketCliente = accept(listenningSocket, (struct sockaddr *) &addr,
			&addrlen);

	//Socket a kernel (listener)

	//Handshake de kernel
	if (!recibir_handshake(KERNEL, socketCliente)) {
		log_warning(g_logger, "Handshake invalido");
		return 0;
	}

	enviar_handshake(numero_memoria, socketCliente);

	int status = 1;		// Estructura que manjea el status de los recieve.

	log_debug(g_logger, "Cliente conectado. Esperando Envío de mensajessss.");
	//Handshake de kernel

	//Describe a kernel
	/*
	 if (es_primera_memoria) {
	 char* serializedPackage;
	 serializedPackage = serializarDescribe(&describeRecibido);
	 send(socketCliente, serializedPackage,
	 describeRecibido.cant_tablas * sizeof(t_metadata)
	 + sizeof(describeRecibido.cant_tablas), 0);
	 dispose_package(&serializedPackage);
	 free(describeRecibido.tablas);
	 }
	 */
	//Describe a kernel
	//thread ingreso por consola
	pthread_t threadL;
	int iret1;

	iret1 = pthread_create(&threadL, NULL, inputFunc, (void*) lfsSocket);
	if (iret1) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n", iret1);
		exit(EXIT_FAILURE);
	}
	//thread ingreso por consola

	int headerRecibido;
	int comando_valido;

	while (status) {
		headerRecibido = recieve_header(socketCliente);

		status = headerRecibido;

		if (status) {
			if (headerRecibido == SELECT) {
				t_PackageSelect package;
				package.header = SELECT;
				status = recieve_and_deserialize_select(&package,
						socketCliente);

				comando_valido = ejectuarComando(headerRecibido, &package,
						lfsSocket);
				//send_package(package.header, &package, lfsSocket);

				free(package.tabla);
			} else if (headerRecibido == INSERT) {
				t_PackageInsert package;
				package.header = INSERT;
				status = recieve_and_deserialize_insert(&package,
						socketCliente);

				comando_valido = ejectuarComando(headerRecibido, &package,
						lfsSocket);
				free(package.value);
				free(package.tabla);
				//send_package(package.header, &package, lfsSocket);
			} else if (headerRecibido == CREATE) {
				t_PackageCreate package;
				package.header = CREATE;
				status = recieve_and_deserialize_create(&package,
						socketCliente);

				comando_valido = ejectuarComando(headerRecibido, &package,
						lfsSocket);
				//send_package(package.header, &package, lfsSocket);
			} else if (headerRecibido == DESCRIBE) {
				t_PackageDescribe package;
				package.header = DESCRIBE;
				status = recieve_and_deserialize_describe_request(&package,
						socketCliente);

				comando_valido = ejectuarComando(headerRecibido, &package,
						lfsSocket);
				free(package.nombre_tabla);
				//send_package(package.header, &package, lfsSocket);
			}
		}
	}

	log_warning(g_logger, "Kernel Desconectado.");

	pthread_join(&threadL, NULL);

	close(socketCliente);
	close(listenningSocket);
	destruirTablas();
	list_destroy(tabla_segmentos);
	free(memoriaPrincipal);
	free(tabla_paginas.renglones);
	free(bit_map);
	log_destroy(g_logger);
	free(conf_path);

	return 0;
}

void destruirTablas() {

	void destruir(Segmento *segmento) {
		list_destroy(segmento->numeros_pagina);
		free(segmento);
	}

	list_iterate(tabla_segmentos, destruir);
}

void printearTablas() {

	void printear(Segmento *segmento) {
		log_info(g_logger, "Tabla %s", segmento->path);
	}
	;

	list_iterate(tabla_segmentos, &printear);
}

Segmento* buscarSegmento(char* tablaABuscar) {

	int esDeLaTabla(Segmento *segmento) {
		if (strcmp(segmento->path, tablaABuscar) == 0) {
			return 1;
		}
		return 0;
	}
	;

	return (Segmento*) list_find(tabla_segmentos, (int) &esDeLaTabla);
}

void* buscarPagina(int keyBuscado, Segmento* segmento, int* numerodePagina) {

	typedef struct Pagina {
		long timeStamp;
		uint16_t key;
		char value[max_value_size];
	} Pagina;

	Pagina* paginaEncontrada = NULL;

	void esDeLaTabla(int num_pag) {
		Renglon_pagina* renglon = tabla_paginas.renglones + num_pag;
		int offsetPagina = renglon->offset;
		int keyPagina = ((Pagina*) (memoriaPrincipal + offsetPagina))->key;

		if (keyBuscado == keyPagina) {
			paginaEncontrada = ((Pagina*) (memoriaPrincipal + offsetPagina));
			*numerodePagina = num_pag;
		}

	}
	;

	list_iterate(segmento->numeros_pagina, &esDeLaTabla);

	return (void*) paginaEncontrada;
}

int ejectuarComando(int header, void* package, int socket) {
	switch (header) {
	case SELECT:
		return ejecutarSelect((t_PackageSelect*) package, socket);
		break;
	case INSERT:
		return ejecutarInsert((t_PackageInsert*) package, socket);
		break;
	case CREATE:
		return ejecutarCreate((t_PackageCreate*) package, socket);
		break;
	case DESCRIBE:
		return ejecutarDescribe((t_PackageDescribe*) package, socket);
		break;
	}
	return -1;
}

int ejecutarSelect(t_PackageSelect* select, int lfs_socket) {

	typedef struct Pagina {
		long timeStamp;
		uint16_t key;
		char value[max_value_size];
	} Pagina;

	Segmento* segmento_encontrado = buscarSegmento(select->tabla);
	if (segmento_encontrado != NULL) {

		int num_pag;
		Pagina* pagina_encontrada = (Pagina*) buscarPagina(select->key,
				segmento_encontrado, &num_pag);

		if (pagina_encontrada != NULL) {
			log_debug(g_logger, "Registro: TimeStamp: %d, Key:%d, Value: %s",
					pagina_encontrada->timeStamp, pagina_encontrada->key,
					pagina_encontrada->value);

			char* mensaje_a_enviar = string_from_format(
					"Registro: TimeStamp: %d, Key:%d, Value: %s",
					pagina_encontrada->timeStamp, pagina_encontrada->key,
					pagina_encontrada->value);

			enviarMensaje(mensaje_a_enviar, socketCliente);

			free(mensaje_a_enviar);

		} else {

			int status;
			log_info(g_logger, "No tengo ese registro");

			recibir_y_ejecutar(select, lfs_socket);

		}
	} else {
		log_info(g_logger, "Memoria no tiene esa tabla, pidiendo a LFS...");
		recibir_y_ejecutar(select, lfs_socket);

	}

	return 1;
}

void enviarMensaje(char* mensaje, int socket) {
	int total_size;
	char* serializedMesagge = serializarMensaje(mensaje, &total_size);

	usleep(retardo_mem);

	send(socket, serializedMesagge, total_size, 0);
	dispose_package(&serializedMesagge);
}

int recibir_y_ejecutar(t_PackageSelect* paquete, int socket) {

	send_package(paquete->header, paquete, socket);

	t_PackageInsert* paquete_insert;
	paquete_insert = malloc(sizeof(t_PackageInsert));

	paquete_insert->header = INSERT;
	paquete_insert->tabla_long = paquete->tabla_long;

	paquete_insert->tabla = malloc(paquete->tabla_long + 1);
	strcpy(paquete_insert->tabla, paquete->tabla);

	paquete_insert->key = paquete->key;

	t_Respuesta_Select* respuesta_select = malloc(sizeof(t_Respuesta_Select));

	int status = recieve_and_deserialize_RespuestaSelect(respuesta_select,
			socket);

	if (!status) {
		return 0;
	}

	status = respuesta_select->result;

	if (!status) {
		log_warning(g_logger, "Registro no encontrado");

		char* mensaje_a_enviar = string_from_format("Registro no encontrado");
		enviarMensaje(mensaje_a_enviar, socketCliente);
		free(mensaje_a_enviar);

		free(respuesta_select->value);
		free(paquete_insert->tabla);
		dispose_package(&paquete_insert);
		dispose_package(&respuesta_select);

		return 0;
	}

	paquete_insert->value = malloc(respuesta_select->value_long + 1);
	strcpy(paquete_insert->value, respuesta_select->value);

	paquete_insert->value_long = respuesta_select->value_long;
	paquete_insert->timestamp = respuesta_select->timestamp;
	paquete_insert->total_size = sizeof(paquete_insert->header)
			+ sizeof(paquete_insert->value_long)
			+ sizeof(paquete_insert->tabla_long) + sizeof(paquete_insert->key)
			+ sizeof(paquete_insert->timestamp) + paquete_insert->value_long
			+ paquete_insert->tabla_long;

	ejecutarInsert(paquete_insert);

	log_debug(g_logger, "Registro: TimeStamp: %d, Key:%d, Value: %s",
			paquete_insert->timestamp, paquete_insert->key,
			paquete_insert->value);

	char* mensaje_a_enviar = string_from_format(
			"Registro: TimeStamp: %d, Key:%d, Value: %s",
			paquete_insert->timestamp, paquete_insert->key,
			paquete_insert->value);
	enviarMensaje(mensaje_a_enviar, socketCliente);
	free(mensaje_a_enviar);

	free(respuesta_select->value);
	free(paquete_insert->tabla);
	free(paquete_insert->value);
	dispose_package(&paquete_insert);
	dispose_package(&respuesta_select);

	return status;
}

int primerpaginaLibre() {
	int bit;
	for (int pag = 0; pag < cant_paginas; pag++) {
		bit = bit_map[pag];
		if (!bit) {
			return pag;
		}
	}
	return -1;
}

int ejecutarInsert(t_PackageInsert* insert) {

	typedef struct Pagina {
		long timeStamp;
		uint16_t key;
		char value[max_value_size];
	} Pagina;

	if (strlen(insert->value) > max_value_size) {
		log_warning(g_logger, "Tamaño de value mayor al permitido");
		return -1;
	}

	Segmento* segmento_encontrado = buscarSegmento(insert->tabla);
	if (segmento_encontrado != NULL) {

		int num_pag;
		Pagina* pagina_encontrada = buscarPagina(insert->key,
				segmento_encontrado, &num_pag);

		if (pagina_encontrada != NULL) {

			pagina_encontrada->timeStamp = insert->timestamp;
			strcpy(pagina_encontrada->value, insert->value);

			tabla_paginas.renglones[num_pag].modificado = 1;

		} else {
			int numero_pagina = primerpaginaLibre();

			if (numero_pagina != -1) {

				Pagina* paginaNueva = (Pagina*) (memoriaPrincipal
						+ numero_pagina * sizeof(Pagina));
				paginaNueva->key = insert->key;
				paginaNueva->timeStamp = insert->timestamp;

				strcpy(paginaNueva->value, insert->value);

				tabla_paginas.renglones[numero_pagina].numero = numero_pagina;
				tabla_paginas.renglones[numero_pagina].modificado = 0;

				tabla_paginas.renglones[numero_pagina].offset =
						tabla_paginas.renglones[numero_pagina].numero
								* sizeof(Pagina);

				bit_map[numero_pagina] = 1;

				list_add(segmento_encontrado->numeros_pagina, numero_pagina);

			} else {
				//criterio
			}
		}
	} else {

		Segmento *nuevo_segmento = malloc(sizeof(Segmento));

		strcpy(nuevo_segmento->path, insert->tabla);

		nuevo_segmento->numeros_pagina = list_create();

		int numero_pagina = primerpaginaLibre();

		if (numero_pagina != -1) {
			Pagina* paginaNueva = (Pagina*) (memoriaPrincipal
					+ numero_pagina * sizeof(Pagina));
			paginaNueva->key = insert->key;
			paginaNueva->timeStamp = insert->timestamp;

			strcpy(paginaNueva->value, insert->value);

			tabla_paginas.renglones[numero_pagina].numero = numero_pagina;
			tabla_paginas.renglones[numero_pagina].modificado = 0;

			tabla_paginas.renglones[numero_pagina].offset = numero_pagina
					* sizeof(Pagina);

			bit_map[numero_pagina] = 1;

			list_add(nuevo_segmento->numeros_pagina, numero_pagina);
			list_add(tabla_segmentos, (Segmento*) nuevo_segmento);
		} else {
			//criterio
		}

	}
	return 1;
}

int ejecutarCreate(t_PackageCreate* paquete, int lfs_socket) {

	send_package(CREATE, paquete, lfs_socket);
	return 1;
}

int ejecutarDescribe(t_PackageDescribe* paquete, int lfs_socket) {

	send_package(DESCRIBE, paquete, lfs_socket);
	return 1;
}

void abrir_con(t_config** g_config) {
	(*g_config) = config_create(conf_path);
}

void abrir_log(void) {

	g_logger = log_create("memory_global.log", "memory", 1, LOG_LEVEL_DEBUG);

}

void send_package(int header, void* package, int lfsSocket) {

	char* serializedPackage;
	switch (header) {
	case SELECT:
		serializedPackage = serializarSelect((t_PackageSelect*) package);
		usleep(retardo_fs);
		send(lfsSocket, serializedPackage,
				((t_PackageSelect*) package)->total_size, 0);

		break;
	case INSERT:
		serializedPackage = serializarInsert((t_PackageInsert*) package);
		usleep(retardo_fs);
		send(lfsSocket, serializedPackage,
				((t_PackageInsert*) package)->total_size, 0);

		break;

	case CREATE:
		serializedPackage = serializarCreate((t_PackageCreate*) package);
		usleep(retardo_fs);
		send(lfsSocket, serializedPackage,
				((t_PackageCreate*) package)->total_size, 0);

		break;
	case DESCRIBE:

		serializedPackage = serializarRequestDescribe(
				(t_PackageDescribe*) package);
		usleep(retardo_fs);
		send(lfsSocket, serializedPackage,
				((t_PackageDescribe*) package)->total_size, 0);

		t_describe describeRecibido;
		recieve_and_deserialize_describe(&describeRecibido, lfsSocket);

		/*
		 for (int i = 0; i < describeRecibido.cant_tablas; i++) {
		 printf("%s\n", describeRecibido.tablas[i].nombre_tabla);
		 }
		 */
		char* serializedPackage2;
		serializedPackage2 = serializarDescribe(&describeRecibido);

		usleep(retardo_mem);

		send(socketCliente, serializedPackage2,
				describeRecibido.cant_tablas * sizeof(t_metadata)
						+ sizeof(describeRecibido.cant_tablas), 0);
		dispose_package(&serializedPackage2);
		free(describeRecibido.tablas);

		break;
	}
	dispose_package(&serializedPackage);

}

void *inputFunc(void* serverSocket)

{
	int enviar = 1;
	int entradaValida;
	t_PackagePosta package;
	package.message = malloc(MAX_MESSAGE_SIZE);
	char *serializedPackage;

	printf(
			"Bienvenido al sistema, puede comenzar a escribir. Escriba 'exit' para salir.\n");

	while (enviar) {
		entradaValida = 1;

		char* entrada;

		entrada = readline("Memory> ");


		char* parametros;
		int header;

		separarEntrada(entrada, &header, &parametros);

		if (header == EXIT_CONSOLE) {
			enviar = 0;
		} else if (header == ERROR) {
			log_warning(g_logger, "Comando no reconocido");
			entradaValida = 0;
		}

		add_history(entrada);

		free(entrada);

		if (enviar && entradaValida) {

			interpretarComando(header, parametros, serverSocket);
			free(parametros);
		}

	}

	log_warning(g_logger, "Desconectado.");

	free(package.message);

}

void interpretarComando(int header, char* parametros, int serverSocket) {

	switch (header) {
	case SELECT:
		select_memory(parametros, serverSocket);
		break;
	case INSERT:
		insert_memory(parametros, serverSocket);
		break;
	case DESCRIBE:
		//describe(parametros, serverSocket);
		break;
	case DROP:
		//drop(parametros, serverSocket);
		break;
	case CREATE:
		create(parametros, serverSocket);
		break;
	case -1:
		break;
	}

}

void select_memory(char* parametros, int serverSocket) {

	int entradaValida = 1;
	int comando_valido;
	t_PackageSelect package;

	if (!fill_package_select(&package, parametros)) {

		log_warning(g_logger, "Incorrecta cantidad de parametros");
		entradaValida = 0;
	}
	if (entradaValida) {
		comando_valido = ejectuarComando(SELECT, &package, serverSocket);
	}
	free(package.tabla);
}

void create(char* parametros, int serverSocket) {

	int entradaValida = 1;
	int comando_valido;
	t_PackageCreate package;

	if (!fill_package_create(&package, parametros)) {
		log_warning(g_logger, "Incorrecta cantidad de parametros");
		entradaValida = 0;
	}
	if (entradaValida) {
		comando_valido = ejectuarComando(CREATE, &package, serverSocket);
	}
	free(package.tabla);
}

void insert_memory(char* parametros, int serverSocket) {

	int entradaValida = 1;
	int comando_valido;
	t_PackageInsert package;

	if (!fill_package_insert(&package, parametros, 0)) {
		log_warning(g_logger, "Incorrecta cantidad de parametros");
		entradaValida = 0;
	}

	if (entradaValida) {

		comando_valido = ejectuarComando(INSERT, &package, serverSocket);

		free(package.tabla);
	}
}
