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
pthread_mutex_t lock;

int cant_paginas;
void* memoriaPrincipal;
t_list* tabla_segmentos;
Tabla_paginas tabla_paginas;
char* bit_map;

int main() {

	struct addrinfo hints;
	struct addrinfo *serverInfo;
	char* ip;
	char* puerto;

	cant_paginas = floor(MEMORY_SIZE / sizeof(Pagina));
	memoriaPrincipal = malloc(MEMORY_SIZE);
	tabla_segmentos = list_create();
	tabla_paginas.renglones = malloc(cant_paginas * sizeof(Renglon_pagina));

	bit_map = malloc(cant_paginas);
	for (int pag = 0; pag < cant_paginas; pag++) {
		bit_map[pag] = 0;
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
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC; // Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	abrir_log();

	t_config *conection_conf;
	abrir_con(&conection_conf);

	ip = config_get_string_value(conection_conf, "IP");
	puerto = config_get_string_value(conection_conf, "PUERTO_DEST");

	getaddrinfo(ip, puerto, &hints, &serverInfo);// Carga en serverInfo los datos de la conexion

	config_destroy(conection_conf);

	int lfsSocket;
	lfsSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol);

	connect(lfsSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);
	freeaddrinfo(serverInfo);	// No lo necesitamos mas

	printf("Conectado al LFS \n");

	enviar_handshake(MEMORY, lfsSocket);

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

	if (!recibir_handshake(KERNEL, socketCliente)) {
		printf("Handshake invalido \n");
		return 0;
	}

	int status = 1;		// Estructura que manjea el status de los recieve.

	printf("Cliente conectado. Esperando Envío de mensajessss.\n");

	t_describe describe;
	t_metadata meta;
	meta.consistencia = SC;
	strcpy(meta.nombre_tabla, "TABLA_STRONG");
	t_metadata meta2;
	meta2.consistencia = EC;
	strcpy(meta2.nombre_tabla, "TABLA_EVENTUAL");
	describe.cant_tablas = 2;
	describe.tablas = malloc(2 * sizeof(t_metadata));
	describe.tablas[0] = meta;
	describe.tablas[1] = meta2;

	printf("Tabla %s \n", describe.tablas[0].nombre_tabla);
	printf("Tabla %s \n", describe.tablas[1].nombre_tabla);

	char* serializedPackage;
	serializedPackage = serializarDescribe(&describe);
	send(socketCliente, serializedPackage,
			2 * sizeof(t_metadata) + sizeof(describe.cant_tablas), 0);
	dispose_package(&serializedPackage);
	free(describe.tablas);

	/*
	 //thread
	 pthread_t threadL;
	 int iret1;

	 if (pthread_mutex_init(&lock, NULL) != 0) {
	 printf("\n mutex init failed\n");
	 return 1;
	 }

	 iret1 = pthread_create(&threadL, NULL, inputFunc, (void*) lfsSocket);
	 if (iret1) {
	 fprintf(stderr, "Error - pthread_create() return code: %d\n", iret1);
	 exit(EXIT_FAILURE);
	 }

	 */
	//thread
	int headerRecibido;

	while (status) {
		headerRecibido = recieve_header(socketCliente);

		status = headerRecibido;

		if (status) {
			if (headerRecibido == SELECT) {
				t_PackageSelect package;
				status = recieve_and_deserialize_select(&package,
						socketCliente);

				ejectuarComando(headerRecibido, &package);

				package.header = SELECT;
				send_package(headerRecibido, &package, lfsSocket);

				free(package.tabla);
			} else if (headerRecibido == INSERT) {
				t_PackageInsert package;
				status = recieve_and_deserialize_insert(&package,
						socketCliente);

				ejectuarComando(headerRecibido, &package);
				package.header = INSERT;

				send_package(headerRecibido, &package, lfsSocket);
			}

		}

		//pthread_mutex_lock(&lock);
		//status = recieve_and_deserialize(&package, socketCliente);
		//pthread_mutex_unlock(&lock);

		//fill_package(&packageEnvio, &username);

		// Ver el "Deserializando estructuras dinamicas" en el comentario de la funcion.

	}

	printf("Cliente Desconectado.\n");
	/*
	 * 	Terminado el intercambio de paquetes, cerramos todas las conexiones y nos vamos a mirar Game of Thrones, que seguro nos vamos a divertir mas...
	 *
	 *
	 * 																					~ Divertido es Disney ~
	 *
	 */
	//pthread_exit(&threadL);
	//pthread_mutex_destroy(&lock);
	close(socketCliente);
	close(listenningSocket);
	list_destroy(tabla_segmentos);
	free(memoriaPrincipal);
	free(tabla_paginas.renglones);
	log_destroy(g_logger);

	/* See ya! */

	return 0;
}

void printearTablas() {

	void printear(Segmento *segmento) {
		printf("%s.\n",segmento->path);
	};

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

Pagina* buscarPagina(int keyBuscado, Segmento* segmento, int* numerodePagina) {

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

	return paginaEncontrada;
}

void ejectuarComando(int header, void* package) {
	switch (header) {
	case SELECT:
		ejecutarSelect((t_PackageSelect*) package);
		break;
	case INSERT:
		ejecutarInsert((t_PackageInsert*) package);

		break;
	}
}

void ejecutarSelect(t_PackageSelect* select) {
	Segmento* segmento_encontrado = buscarSegmento(select->tabla);
	if (segmento_encontrado != NULL) {

		int num_pag;
		Pagina* pagina_encontrada = buscarPagina(select->key,
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

void ejecutarInsert(t_PackageInsert* insert) {

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

				tabla_paginas.renglones[numero_pagina].offset = tabla_paginas.renglones[numero_pagina].numero * sizeof(Pagina);

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

			tabla_paginas.renglones[numero_pagina].offset = numero_pagina * sizeof(Pagina);

			bit_map[numero_pagina] = 1;

			list_add(nuevo_segmento->numeros_pagina, numero_pagina);
			list_add(tabla_segmentos, (Segmento*)nuevo_segmento);
		} else {
			//criterio
		}

	}
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

/*
 void *inputFunc(void* serverSocket)

 {
 int enviar = 1;
 t_PackagePosta package;
 package.message = malloc(MAX_MESSAGE_SIZE);
 char *serializedPackage;

 //t_PackageRec packageRec;
 //int status = 1;		// Estructura que manjea el status de los recieve.

 printf(
 "Bienvenido al sistema, puede comenzar a escribir. Escriba 'exit' para salir.\n");

 int ingresoCorrecto;
 while (enviar) {

 ingresoCorrecto = 1;
 fill_package(&package); // Completamos el package, que contendra los datos del mensaje que vamos a enviar.

 if (package.header == ERROR) {
 ingresoCorrecto = 0;
 printf("Comando no reconocido\n");
 } 		// Chequeamos si el usuario quiere salir.

 if (package.header == -1) {
 enviar = 0;
 } 		// Chequeamos si el usuario quiere salir.

 if (enviar && ingresoCorrecto) {
 serializedPackage = serializarOperandos(&package);// Ver: ������Por que serializacion dinamica? En el comentario de la definicion de la funcion.
 send((int) serverSocket, serializedPackage, package.total_size, 0);
 dispose_package(&serializedPackage);

 //status = recieve_and_deserialize(&packageRec, serverSocket);
 //if (status) printf("%s says: %s", packageRec.username, packageRec.message);
 }

 }

 printf("Desconectado.\n");


 free(package.message);

 }

 */

/*
 typedef struct t_Package {
 uint32_t message_long;
 char* message;
 uint32_t total_size;
 } t_Package;
 */
