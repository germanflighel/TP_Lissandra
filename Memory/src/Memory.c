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
void *intercambiarTabla();
void* journalCadaX();
void* watch_config(char* config);

struct addrinfo hints;

t_config *conection_conf;

int numero_memoria;
int memoryUP = 1;

//Estructura Memoria Principal
int cant_marcos;
void* memoriaPrincipal;
t_list* tabla_segmentos;
char* bit_map;
int max_value_size;
double memory_size;
char* conf_path;

pthread_mutex_t memoria_mutex;
pthread_mutex_t segmentos_mutex;
pthread_mutex_t bitmap_mutex;

//Estructura Memoria Principal

char* puerto_propio;
int lfsSocket;

int retardo_mem;
int retardo_fs;
int retardo_gossiping;
int retardo_journal;
pthread_mutex_t config_mutex;

pthread_mutex_t journaling;

pthread_mutex_t gossiping_mutex;
t_list* tablaGossiping;
t_list* tablaSeeds;

int main() {

	//Estructura Conexiones
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
	conf_path = malloc(strlen(entrada) + 1);
	strcpy(conf_path, entrada);
	free(entrada);

	//Cofig Path

	//Abro log y config
	abrir_log();

	abrir_con(&conection_conf);
	printf("Archivo de configuración \"%s\" levantado\n", conf_path);
	//Abro log y config

	memory_size = config_get_double_value(conection_conf, "TAM_MEM");
	numero_memoria = config_get_int_value(conection_conf, "MEMORY_NUMBER");

	retardo_mem = 1000 * config_get_int_value(conection_conf, "RETARDO_MEM");
	retardo_fs = 1000 * config_get_int_value(conection_conf, "RETARDO_FS");
	retardo_gossiping = 1000
			* config_get_int_value(conection_conf, "RETARDO_GOSSIPING");
	retardo_journal = 1000
			* config_get_int_value(conection_conf, "RETARDO_JOURNAL");

	pthread_mutex_init(&config_mutex, NULL);

	ip = config_get_string_value(conection_conf, "IP");
	puerto = config_get_string_value(conection_conf, "PUERTO_FS");
	puerto_propio = config_get_string_value(conection_conf, "PUERTO");

	//gossping table init
	char** ip_seed = config_get_array_value(conection_conf, "IP_SEEDS");
	char** puerto_seed = config_get_array_value(conection_conf, "PUERTO_SEEDS");

	pthread_mutex_init(&gossiping_mutex, NULL);
	tablaGossiping = list_create();

	tablaSeeds = list_create();

	int index = 0;

	while (ip_seed[index] != NULL) {
		Seed* seed = malloc(sizeof(Seed));
		strcpy(seed->ip, ip_seed[index]);
		strcpy(seed->puerto, puerto_seed[index]);
		list_add(tablaSeeds, seed);
		index++;
	}

	char* local = getLocalIp();
	log_debug(g_logger, "IP %s", local);

	Seed* seed = malloc(sizeof(Seed));
	strcpy(seed->ip, "127.0.0.1");
	strcpy(seed->puerto, puerto_propio);
	list_add(tablaGossiping, seed);

	free(local);
	//gossping table init

	log_info(g_logger, puerto_propio);

	getaddrinfo(ip, puerto, &hints, &serverInfo);// Carga en serverInfo los datos de la conexion

	//Cierro config

	//Socket a lfs
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

	//Handshake con LFS
	//inicializacion memoria
	pthread_mutex_init(&memoria_mutex, NULL);
	pthread_mutex_init(&segmentos_mutex, NULL);
	pthread_mutex_init(&bitmap_mutex, NULL);

	typedef struct Pagina {
		unsigned long long timeStamp;
		uint16_t key;
		char value[max_value_size];
	} Pagina;

	cant_marcos = floor(memory_size / (double) sizeof(Pagina));
	//printf("%d \n", cant_marcos);
	memoriaPrincipal = malloc(memory_size);
	tabla_segmentos = list_create();

	//tabla_paginas.renglones = malloc(cant_marcos * sizeof(Renglon_pagina));

	bit_map = malloc(cant_marcos);
	liberarBitMap();

	//inicializacion memoria

	//thread inotify
	pthread_t threadInotify;
	int inoret;

	inoret = pthread_create(&threadInotify, NULL, (void*) watch_config,
			conf_path);
	if (inoret) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n", inoret);
		exit(EXIT_FAILURE);
	}
	//thread inotify

	//thread gossping
	pthread_t threadGossping;
	int gosippingret;

	gosippingret = pthread_create(&threadGossping, NULL, intercambiarTabla,
	NULL);
	if (gosippingret) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n",
				gosippingret);
		exit(EXIT_FAILURE);
	}
	//thread gossping

	//thread journaling
	pthread_t threadJournaling;
	int jounralret;

	jounralret = pthread_create(&threadJournaling, NULL, journalCadaX,
	NULL);
	if (jounralret) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n",
				jounralret);
		exit(EXIT_FAILURE);
	}
	//thread journaling

	//thread ingreso por consola
	pthread_t threadL;
	int iret1;

	iret1 = pthread_create(&threadL, NULL, inputFunc, (void*) lfsSocket);
	if (iret1) {
		fprintf(stderr, "Error - pthread_create() return code: %d\n", iret1);
		exit(EXIT_FAILURE);
	}
	//thread ingreso por consola

	fd_set readset, tempset;
	int maxfd, flags;
	int srvsock, peersock, j, result, result1, sent;
	socklen_t len;
	struct timeval tv;
	//char buffer[MAX_BUFFER_SIZE+1];
	struct sockaddr_in addr;

	pthread_mutex_init(&journaling, NULL);

	//binding

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;		// No importa si uso IPv4 o IPv6
	hints.ai_flags = AI_PASSIVE;// Asigna el address del localhost: 127.0.0.1
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	getaddrinfo(NULL, puerto_propio, &hints, &serverInfo); // Notar que le pasamos NULL como IP, ya que le indicamos que use localhost en AI_PASSIVE

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

	srvsock = listenningSocket;

	listen(srvsock, BACKLOG);

	//binding

	FD_ZERO(&readset);
	FD_SET(srvsock, &readset);
	maxfd = srvsock;

	do {
		memcpy(&tempset, &readset, sizeof(tempset));
		tv.tv_sec = 10;
		tv.tv_usec = 0;
		result = select(maxfd + 1, &tempset, NULL, NULL, &tv);

		if (result == 0) {
			//printf("Han pasado 10 segundos sin novedades de Kernel !\n");
		} else if (result < 0 && errno != EINTR) {
			printf("Error in select(): %s\n", strerror(errno));
		} else if (result > 0) {

			if (FD_ISSET(srvsock, &tempset)) {
				len = sizeof(addr);
				peersock = accept(srvsock, &addr, &len);
				if (peersock < 0) {
					printf("Error in accept(): %s\n", strerror(errno));
				} else {
					FD_SET(peersock, &readset);
					maxfd = (maxfd < peersock) ? peersock : maxfd;

					//log_info(g_logger, "Conectado socket: %d", peersock);

					int handshake_id = handshake_recibido(peersock);

					if (handshake_id == KERNEL) {
						enviar_handshake(numero_memoria, peersock);

						//log_debug(g_logger,"Cliente conectado. Esperando Envío de mensajes.");
					} else if (handshake_id == MEMORY) {

						t_PackageSeeds* seedsAEnviar = listToGossipingPackage();

						char* serializedSeeds = serializarGossipingTable(
								seedsAEnviar);

						send(peersock, serializedSeeds,
								seedsAEnviar->cant_seeds * sizeof(Seed)
										+ sizeof(seedsAEnviar->cant_seeds), 0);

						free(serializedSeeds);
						free(seedsAEnviar->seeds);

						t_PackageSeeds* seeds = malloc(sizeof(t_PackageSeeds));
						recieve_and_deserialize_gossipingTable(seeds, peersock);

						seedPackageToTable(seeds);

						free(seeds->seeds);
						free(seeds);

					} else {
						log_warning(g_logger, "Handshake invalido");
						return 0;
					}

				}
				FD_CLR(srvsock, &tempset);
			}

			for (j = 0; j < maxfd + 1; j++) {
				if (FD_ISSET(j, &tempset)) {
					//printf("1\n");

					do {
						//printf("2\n");

						result = recieve(j);

					} while (result == -1 && errno == EINTR);

					if (result > 0) {
						//printf("3\n");
						//ok

					} else if (result == 0) {
						//printf("Kernel desconectado\n");
						close(j);
						FD_CLR(j, &readset);
					} else {
						printf("Error in recv(): %s\n", strerror(errno));
					}
				}      // end if (FD_ISSET(j, &tempset))
			}      // end for (j=0;...)
		}      // end else if (result > 0)
	} while (memoryUP);

	log_warning(g_logger, "Termina proceso");

	pthread_detach(threadGossping);
	pthread_detach(threadL);
	pthread_detach(threadJournaling);
	pthread_detach(threadInotify);

	close(listenningSocket);
	destruirTablas();
	list_destroy(tabla_segmentos);
	free(memoriaPrincipal);
	free(bit_map);
	log_destroy(g_logger);
	free(conf_path);

	return 0;
}

void liberarBitMap() {
	pthread_mutex_lock(&bitmap_mutex);
	for (int pag = 0; pag < cant_marcos; pag++) {
		bit_map[pag] = 0;
	}
	pthread_mutex_unlock(&bitmap_mutex);
}

void mostrarGosip() {
	void show(Seed *seed) {
		log_debug(g_logger, "Puerto %s", seed->puerto);
	}

	list_iterate(tablaGossiping, show);
}

char* getLocalIp() {

	char hostbuffer[256];
	char *IPbuffer;
	struct hostent *host_entry;
	int hostname;

	// To retrieve hostname
	hostname = gethostname(hostbuffer, sizeof(hostbuffer));
	checkHostName(hostname);

	// To retrieve host information
	host_entry = gethostbyname(hostbuffer);
	checkHostEntry(host_entry);

	// To convert an Internet network
	// address into ASCII string
	IPbuffer = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0]));

	char* result = malloc(50);
	strcpy(result, IPbuffer);

	return result;

}

// Returns hostname for the local computer
void checkHostName(int hostname) {
	if (hostname == -1) {
		perror("gethostname");
		exit(1);
	}
}

// Returns host information corresponding to host name
void checkHostEntry(struct hostent * hostentry) {
	if (hostentry == NULL) {
		perror("gethostbyname");
		exit(1);
	}
}

// Converts space-delimited IPv4 addresses
// to dotted-decimal format
void checkIPbuffer(char *IPbuffer) {
	if (NULL == IPbuffer) {
		perror("inet_ntoa");
		exit(1);
	}
}

int recieve(int socketCliente) {

	int headerRecibido;
	int comando_valido;
	int status;
	headerRecibido = recieve_header(socketCliente);

	status = headerRecibido;

	if (status) {
		if (headerRecibido == SELECT) {
			t_PackageSelect package;
			package.header = SELECT;
			status = recieve_and_deserialize_select(&package, socketCliente);

			pthread_mutex_lock(&journaling);
			comando_valido = ejectuarComando(headerRecibido, &package,
					socketCliente, 0);
			pthread_mutex_unlock(&journaling);
			//send_package(package.header, &package, lfsSocket);

			free(package.tabla);
		} else if (headerRecibido == INSERT) {
			t_PackageInsert package;
			package.header = INSERT;

			status = recieve_and_deserialize_insert(&package, socketCliente);

			pthread_mutex_lock(&journaling);
			comando_valido = ejectuarComando(headerRecibido, &package,
					socketCliente, 0);
			pthread_mutex_unlock(&journaling);
			free(package.value);
			free(package.tabla);
			//send_package(package.header, &package, lfsSocket);
		} else if (headerRecibido == CREATE) {
			t_PackageCreate package;
			package.header = CREATE;
			status = recieve_and_deserialize_create(&package, socketCliente);

			pthread_mutex_lock(&journaling);
			comando_valido = ejectuarComando(headerRecibido, &package,
					socketCliente, 0);
			pthread_mutex_unlock(&journaling);
			//send_package(package.header, &package, lfsSocket);
		} else if (headerRecibido == DESCRIBE) {
			t_PackageDescribe package;
			package.header = DESCRIBE;
			status = recieve_and_deserialize_describe_request(&package,
					socketCliente);

			pthread_mutex_lock(&journaling);
			comando_valido = ejectuarComando(headerRecibido, &package,
					socketCliente, 0);
			pthread_mutex_unlock(&journaling);
			free(package.nombre_tabla);
			//send_package(package.header, &package, lfsSocket);
		} else if (headerRecibido == DROP) {
			t_PackageDrop package;
			package.header = DROP;
			status = recieve_and_deserialize_drop(&package, socketCliente);

			pthread_mutex_lock(&journaling);
			comando_valido = ejectuarComando(headerRecibido, &package,
					socketCliente, 0);
			pthread_mutex_unlock(&journaling);
			free(package.nombre_tabla);
			//send_package(package.header, &package, lfsSocket);
		} else if (headerRecibido == JOURNAL) {

			log_warning(g_logger, "JOURNAL RECIBIDO");
			journal();
			//send_package(package.header, &package, lfsSocket);
		} else if (headerRecibido == GOSSIPING) {

			t_PackageSeeds* seedsAEnviar = listToGossipingPackage();

			char* serializedSeeds = serializarGossipingTable(seedsAEnviar);

			send(socketCliente, serializedSeeds,
					seedsAEnviar->cant_seeds * sizeof(Seed)
							+ sizeof(seedsAEnviar->cant_seeds), 0);

			free(serializedSeeds);
			free(seedsAEnviar->seeds);

		}
	}
	return status;
}

void destruirTablas() {

	void destruir(Segmento *segmento) {
		list_destroy(segmento->tablaDePaginas);
		free(segmento);
	}

	pthread_mutex_lock(&segmentos_mutex);
	list_iterate(tabla_segmentos, destruir);
	pthread_mutex_unlock(&segmentos_mutex);
}

void printearTablas() {

	void printear(Segmento *segmento) {
		log_info(g_logger, "Tabla %s", segmento->path);
	}
	;

	pthread_mutex_lock(&segmentos_mutex);
	list_iterate(tabla_segmentos, &printear);
	pthread_mutex_unlock(&segmentos_mutex);
}

Segmento* buscarSegmento(char* tablaABuscar) {

	int esDeLaTabla(Segmento *segmento) {
		if (strcmp(segmento->path, tablaABuscar) == 0) {
			return 1;
		}
		return 0;
	}
	;

	pthread_mutex_lock(&segmentos_mutex);
	Segmento* seg = (Segmento*) list_find(tabla_segmentos, (int) &esDeLaTabla);
	pthread_mutex_unlock(&segmentos_mutex);
	return seg;
}

void eliminarSegmento(char* tablaABuscar) {

	int esDeLaTabla(Segmento *segmento) {
		if (strcmp(segmento->path, tablaABuscar) == 0) {
			return 1;
		}
		return 0;
	}
	;

	int destroyer(Segmento *segmento) {
		list_destroy(segmento->tablaDePaginas);
	}
	;

	pthread_mutex_lock(&segmentos_mutex);
	list_remove_and_destroy_by_condition(tabla_segmentos, (int) &esDeLaTabla,
			&destroyer);
	pthread_mutex_unlock(&segmentos_mutex);
}

void* buscarPagina(int keyBuscado, Segmento* segmento, int* numerodePagina) {

	typedef struct Pagina {
		unsigned long long timeStamp;
		uint16_t key;
		char value[max_value_size];
	} Pagina;

	Pagina* paginaEncontrada = NULL;

	void esDeLaTabla(Renglon_pagina* renglon) {
		int offsetPagina = renglon->offset;

		pthread_mutex_lock(&memoria_mutex);
		int keyPagina = ((Pagina*) (memoriaPrincipal + offsetPagina))->key;

		if (keyBuscado == keyPagina) {
			paginaEncontrada = ((Pagina*) (memoriaPrincipal + offsetPagina));
			*numerodePagina = renglon->numero;
		}
		pthread_mutex_unlock(&memoria_mutex);
	}
	;

	list_iterate(segmento->tablaDePaginas, &esDeLaTabla);

	return (void*) paginaEncontrada;
}

int ejectuarComando(int header, void* package, int socket, int esAPI) {
	switch (header) {
	case SELECT:
		return ejecutarSelect((t_PackageSelect*) package, socket, esAPI);
		break;
	case INSERT:
		return ejecutarInsert((t_PackageInsert*) package, socket, 1, !esAPI);
		break;
	case CREATE:
		return ejecutarCreate((t_PackageCreate*) package, socket);
		break;
	case DESCRIBE:
		return ejecutarDescribe((t_PackageDescribe*) package, socket);
		break;
	case DROP:
		return ejecutarDrop((t_PackageDrop*) package, socket);
		break;
	}
	return -1;
}

int ejecutarSelect(t_PackageSelect* select, int socketCliente, int esAPI) {

	typedef struct Pagina {
		unsigned long long timeStamp;
		uint16_t key;
		char value[max_value_size];
	} Pagina;

	Segmento* segmento_encontrado = buscarSegmento(select->tabla);
	if (segmento_encontrado != NULL) {

		int num_pag;
		Pagina* pagina_encontrada = (Pagina*) buscarPagina(select->key,
				segmento_encontrado, &num_pag);

		if (pagina_encontrada != NULL) {

			((Renglon_pagina*) list_get(segmento_encontrado->tablaDePaginas,
					num_pag))->last_used_ts = (unsigned) time(NULL);

			log_debug(g_logger, "Registro: TimeStamp: %llu, Key:%d, Value: %s",
					pagina_encontrada->timeStamp, pagina_encontrada->key,
					pagina_encontrada->value);

			if (!esAPI) {
				char* mensaje_a_enviar = string_from_format(
						"Registro: TimeStamp: %llu, Key:%d, Value: %s",
						pagina_encontrada->timeStamp, pagina_encontrada->key,
						pagina_encontrada->value);

				enviarMensaje(mensaje_a_enviar, socketCliente);

				free(mensaje_a_enviar);
			}

		} else {

			int status;
			log_info(g_logger, "No tengo ese registro");

			recibir_y_ejecutar(select, socketCliente, esAPI);

		}
	} else {
		log_info(g_logger, "Memoria no tiene esa tabla, pidiendo a LFS...");
		recibir_y_ejecutar(select, socketCliente, esAPI);

	}

	return 1;
}

int ejecutarDrop(t_PackageDrop* drop, int socketCliente) {

	eliminarSegmento(drop->nombre_tabla);

	char* serializedPackage = serializarDrop(drop);

	send(lfsSocket, serializedPackage, drop->total_size, 0);

	dispose_package(&serializedPackage);

	return 1;
}

void enviarMensaje(char* mensaje, int socket) {
	int total_size;
	char* serializedMesagge = serializarMensaje(mensaje, &total_size);

	pthread_mutex_lock(&config_mutex);
	int ret = retardo_mem;
	pthread_mutex_unlock(&config_mutex);

	usleep(ret);

	send(socket, serializedMesagge, total_size, 0);
	dispose_package(&serializedMesagge);
}

int recibir_y_ejecutar(t_PackageSelect* paquete, int socketCliente, int esAPI) {

	send_package(paquete->header, paquete, lfsSocket);

	t_PackageInsert* paquete_insert;
	paquete_insert = malloc(sizeof(t_PackageInsert));

	paquete_insert->header = INSERT;
	paquete_insert->tabla_long = paquete->tabla_long;

	paquete_insert->tabla = malloc(paquete->tabla_long + 1);
	strcpy(paquete_insert->tabla, paquete->tabla);

	paquete_insert->key = paquete->key;

	t_Respuesta_Select* respuesta_select = malloc(sizeof(t_Respuesta_Select));

	int status = recieve_and_deserialize_RespuestaSelect(respuesta_select,
			lfsSocket);

	if (!status) {
		return 0;
	}

	status = respuesta_select->result;

	if (!status) {
		log_warning(g_logger, "Registro no encontrado");
		if (!esAPI) {
			char* mensaje_a_enviar = string_from_format(
					"Registro no encontrado");
			enviarMensaje(mensaje_a_enviar, socketCliente);
			free(mensaje_a_enviar);
		}
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

	ejecutarInsert(paquete_insert, 0, NULL, 0);

	log_debug(g_logger, "Registro: TimeStamp: %llu, Key:%d, Value: %s",
			paquete_insert->timestamp, paquete_insert->key,
			paquete_insert->value);

	if (!esAPI) {
		char* mensaje_a_enviar = string_from_format(
				"Registro: TimeStamp: %llu, Key:%d, Value: %s",
				paquete_insert->timestamp, paquete_insert->key,
				paquete_insert->value);
		enviarMensaje(mensaje_a_enviar, socketCliente);
		free(mensaje_a_enviar);
	}
	free(respuesta_select->value);
	free(paquete_insert->tabla);
	free(paquete_insert->value);
	dispose_package(&paquete_insert);
	dispose_package(&respuesta_select);

	return status;
}

int obtenerOLiberarMarco() {
	int bit;
	for (int pag = 0; pag < cant_marcos; pag++) {
		pthread_mutex_lock(&bitmap_mutex);
		bit = bit_map[pag];
		pthread_mutex_unlock(&bitmap_mutex);
		if (!bit) {
			return pag;
		}
	}

	return algoritmoDeReemplazo();
}

int algoritmoDeReemplazo() {

	typedef struct Pagina {
		unsigned long long timeStamp;
		uint16_t key;
		char value[max_value_size];
	} Pagina;

	Renglon_pagina* renglon_encontrado = NULL;
	Segmento* segmento_encontrado = NULL;
	Segmento* segmento_actual = NULL;
	long timestamp = -1;
	int marcoLiberado = -1;

	void porTabla(Renglon_pagina *renglon) {
		if (!renglon->modificado && timestamp >= renglon->last_used_ts) {
			segmento_encontrado = segmento_actual;
			renglon_encontrado = renglon;
			timestamp = renglon->last_used_ts;
		}
	}
	;

	void porSegmento(Segmento *segmento) {
		segmento_actual = segmento;
		list_iterate(segmento->tablaDePaginas, &porTabla);
	}
	;

	int index = 0;
	void ordenar(Renglon_pagina *renglon) {
		renglon->numero = index;
		index++;
	}
	;

	pthread_mutex_lock(&segmentos_mutex);
	list_iterate(tabla_segmentos, &porSegmento);
	pthread_mutex_unlock(&segmentos_mutex);

	if (renglon_encontrado != NULL) {

		list_remove(segmento_encontrado->tablaDePaginas,
				renglon_encontrado->numero);
		list_iterate(segmento_encontrado->tablaDePaginas, &ordenar);

		marcoLiberado = renglon_encontrado->offset / sizeof(Pagina);

		pthread_mutex_lock(&bitmap_mutex);
		bit_map[marcoLiberado] = 0;
		pthread_mutex_unlock(&bitmap_mutex);

	}

	return marcoLiberado;
}

int ejecutarInsert(t_PackageInsert* insert, int socket, int mod,
		int vieneDeKernel) {

	int full = 0;

	typedef struct Pagina {
		unsigned long long timeStamp;
		uint16_t key;
		char value[max_value_size];
	} Pagina;

	if (strlen(insert->value) > max_value_size) {
		log_warning(g_logger, "Tamaño de value mayor al permitido");

		if (vieneDeKernel) {
			enviarMensaje("OK", socket);
		}

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

			((Renglon_pagina*) list_get(segmento_encontrado->tablaDePaginas,
					num_pag))->modificado = 1;
			((Renglon_pagina*) list_get(segmento_encontrado->tablaDePaginas,
					num_pag))->last_used_ts = (unsigned) time(NULL);

		} else {
			int numero_marco = obtenerOLiberarMarco();

			if (numero_marco != -1) {

				pthread_mutex_lock(&memoria_mutex);
				Pagina* paginaNueva = (Pagina*) (memoriaPrincipal
						+ numero_marco * sizeof(Pagina));
				paginaNueva->key = insert->key;
				paginaNueva->timeStamp = insert->timestamp;

				strcpy(paginaNueva->value, insert->value);
				pthread_mutex_unlock(&memoria_mutex);

				Renglon_pagina* entrada_nueva = malloc(sizeof(Renglon_pagina));
				entrada_nueva->modificado = mod;
				entrada_nueva->last_used_ts = (unsigned) time(NULL);
				entrada_nueva->numero =
						segmento_encontrado->tablaDePaginas->elements_count;
				entrada_nueva->offset = numero_marco * sizeof(Pagina);

				pthread_mutex_lock(&bitmap_mutex);
				bit_map[numero_marco] = 1;
				pthread_mutex_unlock(&bitmap_mutex);

				list_add(segmento_encontrado->tablaDePaginas, entrada_nueva);

			} else {

				full = 1;
				log_warning(g_logger, "Memoria full");
				//JOURNALING
			}
		}
	} else {

		Segmento *nuevo_segmento = malloc(sizeof(Segmento));

		strcpy(nuevo_segmento->path, insert->tabla);

		nuevo_segmento->tablaDePaginas = list_create();

		int numero_marco = obtenerOLiberarMarco();

		if (numero_marco != -1) {
			pthread_mutex_lock(&memoria_mutex);
			Pagina* paginaNueva = (Pagina*) (memoriaPrincipal
					+ numero_marco * sizeof(Pagina));
			paginaNueva->key = insert->key;
			paginaNueva->timeStamp = insert->timestamp;

			strcpy(paginaNueva->value, insert->value);
			pthread_mutex_unlock(&memoria_mutex);
			Renglon_pagina* entrada_nueva = malloc(sizeof(Renglon_pagina));
			entrada_nueva->modificado = mod;
			entrada_nueva->last_used_ts = (unsigned) time(NULL);
			entrada_nueva->numero =
					nuevo_segmento->tablaDePaginas->elements_count;
			entrada_nueva->offset = numero_marco * sizeof(Pagina);

			pthread_mutex_lock(&bitmap_mutex);
			bit_map[numero_marco] = 1;
			pthread_mutex_unlock(&bitmap_mutex);

			list_add(nuevo_segmento->tablaDePaginas, entrada_nueva);

			pthread_mutex_lock(&segmentos_mutex);
			list_add(tabla_segmentos, (Segmento*) nuevo_segmento);
			pthread_mutex_unlock(&segmentos_mutex);

		} else {
			full = 1;
			log_warning(g_logger, "Memoria full");
			//JOURNALING
		}

	}
	if (vieneDeKernel) {
		if (full) {
			enviarMensaje("FULL", socket);
		} else {
			enviarMensaje("OK", socket);
		}

	}
	return 1;
}

int ejecutarCreate(t_PackageCreate* paquete, int lfsSocket) {

	send_package(CREATE, paquete, lfsSocket);
	return 1;
}

int ejecutarDescribe(t_PackageDescribe* paquete, int clienteSocket) {

	send_package(DESCRIBE, paquete, clienteSocket);
	return 1;
}

void abrir_con(t_config** g_config) {
	(*g_config) = config_create(conf_path);
}

void abrir_log(void) {
	g_logger = log_create("memory_global.log", "memory", 1, LOG_LEVEL_DEBUG);
}

void journal() {

	typedef struct Pagina {
		unsigned long long timeStamp;
		uint16_t key;
		char value[max_value_size];
	} Pagina;

	pthread_mutex_lock(&journaling);

	char* nombreTabla;

	void porEntrada(Renglon_pagina* renglon) {
		log_debug(g_logger, "Entre");
		if (renglon->modificado) {
			int offsetPagina = renglon->offset;
			pthread_mutex_lock(&memoria_mutex);
			Pagina* pag = ((Pagina*) (memoriaPrincipal + offsetPagina));

			t_PackageInsert* paquete_insert;
			paquete_insert = malloc(sizeof(t_PackageInsert));

			paquete_insert->header = INSERT;
			paquete_insert->tabla_long = strlen(nombreTabla);

			paquete_insert->tabla = malloc(paquete_insert->tabla_long + 1);
			strcpy(paquete_insert->tabla, nombreTabla);

			paquete_insert->key = pag->key;

			paquete_insert->value_long = strlen(pag->value);

			paquete_insert->value = malloc(paquete_insert->value_long + 1);
			strcpy(paquete_insert->value, pag->value);

			paquete_insert->timestamp = pag->timeStamp;
			pthread_mutex_unlock(&memoria_mutex);
			paquete_insert->total_size = sizeof(paquete_insert->header)
					+ sizeof(paquete_insert->value_long)
					+ sizeof(paquete_insert->tabla_long)
					+ sizeof(paquete_insert->key)
					+ sizeof(paquete_insert->timestamp)
					+ paquete_insert->value_long + paquete_insert->tabla_long;

			char* serializedPackage = serializarInsert(paquete_insert);
			send(lfsSocket, serializedPackage, paquete_insert->total_size, 0);

			free(paquete_insert->tabla);
			free(paquete_insert->value);
			free(paquete_insert);
			dispose_package(&serializedPackage);
		}
	}

	void porSegmento(Segmento* seg) {
		nombreTabla = malloc(strlen(seg->path) + 1);
		strcpy(nombreTabla, seg->path);
		log_debug(g_logger, nombreTabla);
		list_iterate(seg->tablaDePaginas, &porEntrada);
		free(nombreTabla);
	}

	pthread_mutex_lock(&segmentos_mutex);
	list_iterate(tabla_segmentos, &porSegmento);
	pthread_mutex_unlock(&segmentos_mutex);

	reinciarMemoria();

	pthread_mutex_unlock(&journaling);
}

void reinciarMemoria() {
	destruirTablas();
	pthread_mutex_lock(&segmentos_mutex);
	list_clean(tabla_segmentos);
	pthread_mutex_unlock(&segmentos_mutex);
	liberarBitMap();
}

void send_package(int header, void* package, int socketCliente) {

	int ret;
	char* serializedPackage;
	switch (header) {
	case SELECT:
		serializedPackage = serializarSelect((t_PackageSelect*) package);

		pthread_mutex_lock(&config_mutex);
		ret = retardo_fs;
		pthread_mutex_unlock(&config_mutex);

		usleep(ret);

		send(lfsSocket, serializedPackage,
				((t_PackageSelect*) package)->total_size, 0);

		break;
	case INSERT:
		serializedPackage = serializarInsert((t_PackageInsert*) package);

		pthread_mutex_lock(&config_mutex);
		ret = retardo_fs;
		pthread_mutex_unlock(&config_mutex);

		usleep(ret);

		send(lfsSocket, serializedPackage,
				((t_PackageInsert*) package)->total_size, 0);

		break;

	case CREATE:
		serializedPackage = serializarCreate((t_PackageCreate*) package);

		pthread_mutex_lock(&config_mutex);
		ret = retardo_fs;
		pthread_mutex_unlock(&config_mutex);

		usleep(ret);

		send(lfsSocket, serializedPackage,
				((t_PackageCreate*) package)->total_size, 0);

		break;
	case DESCRIBE:

		serializedPackage = serializarRequestDescribe(
				(t_PackageDescribe*) package);

		pthread_mutex_lock(&config_mutex);
		ret = retardo_fs;
		pthread_mutex_unlock(&config_mutex);

		usleep(ret);

		send(lfsSocket, serializedPackage,
				((t_PackageDescribe*) package)->total_size, 0);

		t_describe describeRecibido;
		recieve_and_deserialize_describe(&describeRecibido, lfsSocket);

		char* serializedPackage2;
		serializedPackage2 = serializarDescribe(&describeRecibido);

		pthread_mutex_lock(&config_mutex);
		ret = retardo_mem;
		pthread_mutex_unlock(&config_mutex);

		usleep(ret);

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
	int entradaValida;
	t_PackagePosta package;
	package.message = malloc(MAX_MESSAGE_SIZE);
	char *serializedPackage;

	printf(
			"Bienvenido al sistema, puede comenzar a escribir. Escriba 'exit' para salir.\n");

	while (memoryUP) {
		entradaValida = 1;

		char* entrada;

		entrada = readline("Memory> ");

		if (strcmp(entrada, "") != 0) {

			char* parametros;
			int header;

			separarEntrada(entrada, &header, &parametros);

			int okParams = validarParametros(header, parametros);

			if (header == EXIT_CONSOLE) {
				memoryUP = 0;
			} else if (header == ERROR) {
				log_warning(g_logger, "Comando no reconocido");
				entradaValida = 0;
			} else if (!okParams) {
				log_warning(g_logger, "Parametros incorrectos");
			}

			add_history(entrada);

			free(entrada);

			if (memoryUP && entradaValida && okParams) {

				interpretarComando(header, parametros, serverSocket);
				free(parametros);
			}

		}

	}

	log_warning(g_logger, "Desconectado.");
	log_warning(g_logger,
			"Aguarde mientras desconectamos la memoria por favor...");

	free(package.message);

}

t_PackageSeeds* listToGossipingPackage() {

	int cantMems = tablaGossiping->elements_count;
	t_PackageSeeds* seedArray = malloc(sizeof(t_PackageSeeds));
	seedArray->cant_seeds = cantMems;
	seedArray->seeds = malloc(cantMems * sizeof(Seed));
	int i = 0;
	void agregar(Seed* seed) {
		strcpy(seedArray->seeds[i].ip, seed->ip);
		strcpy(seedArray->seeds[i].puerto, seed->puerto);
		i++;
	}
	pthread_mutex_lock(&gossiping_mutex);
	list_iterate(tablaGossiping, &agregar);
	pthread_mutex_unlock(&gossiping_mutex);

	return seedArray;
}

void seedPackageToTable(t_PackageSeeds* seeds) {
	pthread_mutex_lock(&gossiping_mutex);
	for (int i = 0; i < seeds->cant_seeds; i++) {

		int estaLaSeed(Seed* seed) {
			return (strcmp(seed->puerto, seeds->seeds[i].puerto) == 0
					&& strcmp(seed->ip, seeds->seeds[i].ip) == 0);
		}

		if (!list_any_satisfy(tablaGossiping, &estaLaSeed)) {
			Seed* seed = malloc(sizeof(Seed));
			strcpy(seed->puerto, seeds->seeds[i].puerto);
			strcpy(seed->ip, seeds->seeds[i].ip);
			list_add(tablaGossiping, seed);
		};
	}

	//mostrarGosip();

	pthread_mutex_unlock(&gossiping_mutex);
}

void *intercambiarTabla() {
	int serverSocket;

	while (memoryUP) {
		void intercambiarSiFueraPosible(Seed* seed) {

			struct addrinfo *serverInfo;
			getaddrinfo(seed->ip, seed->puerto, &hints, &serverInfo);

			serverSocket = socket(serverInfo->ai_family,
					serverInfo->ai_socktype, serverInfo->ai_protocol);
			if (connect(serverSocket, serverInfo->ai_addr,
					serverInfo->ai_addrlen) == 0) {

				enviar_handshake(MEMORY, serverSocket);

				t_PackageSeeds* seeds = malloc(sizeof(t_PackageSeeds));
				recieve_and_deserialize_gossipingTable(seeds, serverSocket);

				seedPackageToTable(seeds);

				free(seeds->seeds);
				free(seeds);

				t_PackageSeeds* seedsAEnviar = listToGossipingPackage();

				char* serializedSeeds = serializarGossipingTable(seedsAEnviar);

				send(serverSocket, serializedSeeds,
						seedsAEnviar->cant_seeds * sizeof(Seed)
								+ sizeof(seedsAEnviar->cant_seeds), 0);

				free(serializedSeeds);
				free(seedsAEnviar->seeds);

				close(serverSocket);
			}
			freeaddrinfo(serverInfo);
		}

		list_iterate(tablaSeeds, &intercambiarSiFueraPosible);

		pthread_mutex_lock(&config_mutex);
		int ret = retardo_gossiping;
		pthread_mutex_unlock(&config_mutex);

		usleep(ret);

	}
}

void* journalCadaX() {
	while (memoryUP) {
		journal();

		pthread_mutex_lock(&config_mutex);
		int ret = retardo_journal;
		pthread_mutex_unlock(&config_mutex);

		usleep(ret);
	}
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
		comando_valido = ejectuarComando(SELECT, &package, serverSocket, 1);
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
		comando_valido = ejectuarComando(CREATE, &package, serverSocket, 1);
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

		comando_valido = ejectuarComando(INSERT, &package, serverSocket, 1);

		free(package.tabla);
	}
}

void* watch_config(char* config) {
	int wd, fd;

	fd = inotify_init();
	if (fd < 0) {
		perror("Couldn't initialize inotify");
	}

	wd = inotify_add_watch(fd, ".", IN_CREATE | IN_MODIFY | IN_DELETE);
	if (wd == -1) {
		printf("Couldn't add watch to %s\n", config);
	} else {
		printf("Watching:: %s\n", config);
	}

	/* do it forever*/
	while (memoryUP) {
		get_event(fd);
	}

	/* Clean up*/
	inotify_rm_watch(fd, wd);
	close(fd);
	config_destroy(conection_conf);

}

void get_event(int fd) {

	char buffer[BUF_LEN];
	int length, i = 0;

	length = read(fd, buffer, BUF_LEN);
	if (length < 0) {
		perror("read");
	}

	while (i < length) {
		struct inotify_event *event = (struct inotify_event *) &buffer[i];
		if (event->len && !strcmp(event->name, conf_path)) {
			if (event->mask & IN_MODIFY) {
				config_destroy(conection_conf);
				conection_conf = config_create(conf_path);
				pthread_mutex_lock(&config_mutex);
				retardo_mem = 1000
						* config_get_int_value(conection_conf, "RETARDO_MEM");
				retardo_fs = 1000
						* config_get_int_value(conection_conf, "RETARDO_FS");
				retardo_gossiping = 1000
						* config_get_int_value(conection_conf,
								"RETARDO_GOSSIPING");
				retardo_journal = 1000
						* config_get_int_value(conection_conf,
								"RETARDO_JOURNAL");
				pthread_mutex_unlock(&config_mutex);
			}

		}
		i += EVENT_SIZE + event->len;
	}
}
