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
#include "sockets.h"

void *inputFunc(void *);
//pthread_mutex_t lock;

//Estructura Memoria Principal
int cant_paginas;
void* memoriaPrincipal;
t_list* tabla_segmentos;
Tabla_paginas tabla_paginas;
char* bit_map;
int max_value_size;
double memory_size;
//Estructura Memoria Principal

int main() {

	//Estructura Conexiones
	struct addrinfo hints;
	struct addrinfo *serverInfo;
	char* ip;
	char* puerto;
	//Estructura Conexiones

	memory_size = 5000;
	max_value_size = 20;

	//Estructura Conexiones
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC; // Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP
	//Estructura Conexiones

	//Abro log y config
	abrir_log();

	t_config *conection_conf;
	abrir_con(&conection_conf);
	//Abro log y config

	ip = config_get_string_value(conection_conf, "IP");
	puerto = config_get_string_value(conection_conf, "PUERTO_DEST");

	getaddrinfo(ip, puerto, &hints, &serverInfo);// Carga en serverInfo los datos de la conexion

	config_destroy(conection_conf);
	//Cierro config

	//Socket a lfs
	int lfsSocket;
	lfsSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol);

	connect(lfsSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);

	freeaddrinfo(serverInfo);

	printf("Conectado al LFS \n");
	//Socket a lfs

	//Handshake con LFS
	enviar_handshake(MEMORY, lfsSocket);

	t_describe describeRecibido;

	recieve_and_deserialize_describe(&describeRecibido, lfsSocket);
	//Handshake con LFS

	//inicializacion memoria

	//recibir_max_value_size(lfsSocket,&max_value_size);

	typedef struct Pagina {
		long timeStamp;
		uint16_t key;
		char value[max_value_size];
	} Pagina;

	cant_paginas = floor(memory_size / (double) sizeof(Pagina));
	printf("%d \n", cant_paginas);
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

	getaddrinfo(NULL, PUERTO, &hints, &serverInfo); // Notar que le pasamos NULL como IP, ya que le indicamos que use localhost en AI_PASSIVE

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

	listen(listenningSocket, BACKLOG); // IMPORTANTE: listen() es una syscall BLOQUEANTE.

	printf("Esperando kernel... \n");

	struct sockaddr_in addr; // Esta estructura contendra los datos de la conexion del cliente. IP, puerto, etc.
	socklen_t addrlen = sizeof(addr);

	int socketCliente = accept(listenningSocket, (struct sockaddr *) &addr,
			&addrlen);

	//Socket a kernel (listener)

	//Handshake de kernel
	if (!recibir_handshake(KERNEL, socketCliente)) {
		printf("Handshake invalido \n");
		return 0;
	}

	int status = 1;		// Estructura que manjea el status de los recieve.

	printf("Cliente conectado. Esperando Envío de mensajessss.\n");
	//Handshake de kernel

	//Describe a kernel
	char* serializedPackage;
	serializedPackage = serializarDescribe(&describeRecibido);
	send(socketCliente, serializedPackage,
			2 * sizeof(t_metadata) + sizeof(describeRecibido.cant_tablas), 0);
	dispose_package(&serializedPackage);
	free(describeRecibido.tablas);
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
				status = recieve_and_deserialize_select(&package,socketCliente);

				comando_valido = ejectuarComando(headerRecibido, &package);
				send_package(package.header, &package, lfsSocket);

				free(package.tabla);
			} else if (headerRecibido == INSERT) {
				t_PackageInsert package;
				package.header = INSERT;
				status = recieve_and_deserialize_insert(&package,socketCliente);

				comando_valido = ejectuarComando(headerRecibido, &package);
				send_package(package.header, &package, lfsSocket);
			}
		}
	}

	printf("Cliente Desconectado.\n");

	pthread_exit(&threadL);

	close(socketCliente);
	close(listenningSocket);
	destruirTablas();
	list_destroy(tabla_segmentos);
	free(memoriaPrincipal);
	free(tabla_paginas.renglones);
	log_destroy(g_logger);

	return 0;
}




void destruirTablas() {

	void destruir(Segmento *segmento) {
		list_destroy(segmento->numeros_pagina);
	}

	list_iterate(tabla_segmentos, &destruir);
}

void printearTablas() {

	void printear(Segmento *segmento) {
		printf("%s.\n", segmento->path);
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

int ejectuarComando(int header, void* package) {
	switch (header) {
	case SELECT:
		return ejecutarSelect((t_PackageSelect*) package);
		break;
	case INSERT:
		return ejecutarInsert((t_PackageInsert*) package);
		break;
	}
	return -1;
}

int ejecutarSelect(t_PackageSelect* select) {

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
			printf("Registro: TimeStamp: %d, Key:%d, Value: %s \n",
					pagina_encontrada->timeStamp, pagina_encontrada->key,
					pagina_encontrada->value);

		} else {
			printf("No tengo ese registro \n");
			//lo pido a lfs
		}
	} else {
		printf("No tengo esa tabla \n");
		//lo pido a lfs
	}

	return 1;
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
		printf("Tamaño de value mayor al permitido \n");
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

void abrir_con(t_config** g_config) {

	(*g_config) = config_create(CONFIG_PATH);

}

void abrir_log(void) {

	g_logger = log_create("memory_global.log", "memory", 0, LOG_LEVEL_INFO);

}

void send_package(int header, void* package, int lfsSocket) {

	char* serializedPackage;
	switch (header) {
	case SELECT:
		serializedPackage = serializarSelect((t_PackageSelect*) package);
		send(lfsSocket, serializedPackage,
				((t_PackageSelect*) package)->total_size, 0);

		break;
	case INSERT:
		serializedPackage = serializarInsert((t_PackageInsert*) package);
		send(lfsSocket, serializedPackage,
				((t_PackageInsert*) package)->total_size, 0);

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

	//t_PackageRec packageRec;
	//int status = 1;		// Estructura que manjea el status de los recieve.

	printf(
			"Bienvenido al sistema, puede comenzar a escribir. Escriba 'exit' para salir.\n");

	while (enviar) {
		entradaValida = 1;

		char* entrada = leerConsola();

		char* parametros;
		int header;

		separarEntrada(entrada, &header, &parametros);

		if (header == EXIT_CONSOLE) {
			enviar = 0;
		} else if (header == ERROR) {
			printf("Comando no reconocido\n");
			entradaValida = 0;
		}

		free(entrada);

		if (enviar && entradaValida) {

			interpretarComando(header, parametros, serverSocket);
			free(parametros);
			/*
			 if (header == SELECT) {
			 t_PackageSelect package;
			 if (!fill_package_select(&package, parametros)) {
			 printf("Incorrecta cantidad de parametros\n");
			 entradaValida = 0;
			 }

			 if (entradaValida) {
			 printf("SELECT enviado (Tabla: %s, Key: %d)\n",
			 package.tabla, package.key);

			 serializedPackage = serializarSelect(&package);

			 send(serverSocket, serializedPackage, package.total_size,
			 0);

			 free(package.tabla);
			 dispose_package(&serializedPackage);
			 }
			 } else if (header == INSERT) {
			 t_PackageInsert package;
			 if (!fill_package_insert(&package, parametros,0)) {
			 printf("Incorrecta cantidad de parametros\n");
			 entradaValida = 0;
			 }

			 if (entradaValida) {
			 printf("INSERT enviado (Tabla: %s, Key: %d, Value: %s, Timestamp: %d)\n", package.tabla,
			 package.key,package.value,package.timestamp);

			 serializedPackage = serializarInsert(&package);

			 send(serverSocket, serializedPackage, package.total_size,
			 0);

			 free(package.tabla);
			 dispose_package(&serializedPackage);
			 }
			 }*/

			/*
			 interpretarComando(package.header,package.message);
			 serializedPackage = serializarOperandos(&package);	// Ver: ������Por que serializacion dinamica? En el comentario de la definicion de la funcion.
			 send(serverSocket, serializedPackage, package.total_size, 0);
			 dispose_package(&serializedPackage);
			 */
			//status = recieve_and_deserialize(&packageRec, serverSocket);
			//if (status) printf("%s says: %s", packageRec.username, packageRec.message);
		}

	}

	printf("Desconectado.\n");

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
		//create(parametros, serverSocket);
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

		printf("Incorrecta cantidad de parametros\n");
		entradaValida = 0;
	}
	if (entradaValida) {
		comando_valido = ejectuarComando(SELECT, &package);
	}
	free(package.tabla);
}

void insert_memory(char* parametros, int serverSocket) {

	int entradaValida = 1;
	int comando_valido;
	t_PackageInsert package;

	if (!fill_package_insert(&package, parametros, 0)) {
		printf("Incorrecta cantidad de parametros\n");
		entradaValida = 0;
	}

	if (entradaValida) {

		comando_valido = ejectuarComando(INSERT, &package);

		free(package.tabla);
	}
}

/*
	 //hardcode tablas
	 Pagina franco;
	 franco.timeStamp = 1557972674;
	 franco.key = 20;
	 strcpy(franco.value, "Franco");

	 Pagina santi;
	 santi.timeStamp = 1557972674;
	 santi.key = 21;
	 strcpy(santi.value, "Santi");

	 tabla_paginas.renglones[0].numero = 0;
	 tabla_paginas.renglones[0].modificado = 0;
	 tabla_paginas.renglones[0].offset = tabla_paginas.renglones[0].numero * sizeof(Pagina);
	 bit_map[tabla_paginas.renglones[0].numero] = 1;

	 tabla_paginas.renglones[1].numero = 1;
	 tabla_paginas.renglones[1].modificado = 0;
	 tabla_paginas.renglones[1].offset = tabla_paginas.renglones[1].numero * sizeof(Pagina);
	 bit_map[tabla_paginas.renglones[1].numero] = 1;

	 memcpy((memoriaPrincipal + tabla_paginas.renglones[0].offset), &franco, sizeof(Pagina));
	 memcpy((memoriaPrincipal + tabla_paginas.renglones[1].offset), &santi, sizeof(Pagina));


	 Segmento tabla1;
	 strcpy(tabla1.path, "TABLA1");
	 tabla1.numeros_pagina = list_create();
	 list_add(tabla1.numeros_pagina, 0);
	 list_add(tabla1.numeros_pagina, 1);

	 list_add(tabla_segmentos, &tabla1);

	 //Harcode tablas
	 */
