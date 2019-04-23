/*
 ============================================================================
 Name        : LFS.c
 Author      : 
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "LFS.h"
#include "sockets.h"

int main() {
	/*
	 *
	 *  Estas y otras preguntas existenciales son resueltas getaddrinfo();
	 *
	 *  Obtiene los datos de la direccion de red y lo guarda en serverInfo.
	 *
	 */
	struct addrinfo hints;
	struct addrinfo *serverInfo;

	t_log* logger = iniciar_logger();
	t_config* config = leer_config();

	char* puerto = config_get_string_value(config, "PUERTO");
	log_info(logger, puerto);

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_flags = AI_PASSIVE;		// Asigna el address del localhost: 127.0.0.1
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	getaddrinfo(NULL, puerto, &hints, &serverInfo); // Notar que le pasamos NULL como IP, ya que le indicamos que use localhost en AI_PASSIVE

	/*
	 * 	Descubiertos los misterios de la vida (por lo menos, para la conexion de red actual), necesito enterarme de alguna forma
	 * 	cuales son las conexiones que quieren establecer conmigo.
	 *
	 * 	Para ello, y basandome en el postulado de que en Linux TODO es un archivo, voy a utilizar... Si, un archivo!
	 *
	 * 	Mediante socket(), obtengo el File Descriptor que me proporciona el sistema (un integer identificador).
	 *
	 */
	/* Necesitamos un socket que escuche las conecciones entrantes */
	int listenningSocket;
	listenningSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
			serverInfo->ai_protocol);

	/*
	 * 	Perfecto, ya tengo un archivo que puedo utilizar para analizar las conexiones entrantes. Pero... ������������������Por donde?
	 *
	 * 	Necesito decirle al sistema que voy a utilizar el archivo que me proporciono para escuchar las conexiones por un puerto especifico.
	 *
	 * 				OJO! Todavia no estoy escuchando las conexiones entrantes!
	 *
	 */
	bind(listenningSocket, serverInfo->ai_addr, serverInfo->ai_addrlen);
	freeaddrinfo(serverInfo); // Ya no lao vamos a necesitar

	/*
	 * 	Ya tengo un medio de comunicacion (el socket) y le dije por que "telefono" tiene que esperar las llamadas.
	 *
	 * 	Solo me queda decirle que vaya y escuche!
	 *
	 */
	listen(listenningSocket, BACKLOG); // IMPORTANTE: listen() es una syscall BLOQUEANTE.

	printf("Esperando memoria... \n");

	/*
	 * 	El sistema esperara hasta que reciba una conexion entrante...
	 * 	...
	 * 	...
	 * 	BING!!! Nos estan llamando! ������������������Y ahora?
	 *
	 *	Aceptamos la conexion entrante, y creamos un nuevo socket mediante el cual nos podamos comunicar (que no es mas que un archivo).
	 *
	 *	������������������Por que crear un nuevo socket? Porque el anterior lo necesitamos para escuchar las conexiones entrantes. De la misma forma que
	 *	uno no puede estar hablando por telefono a la vez que esta esperando que lo llamen, un socket no se puede encargar de escuchar
	 *	las conexiones entrantes y ademas comunicarse con un cliente.
	 *
	 *			Nota: Para que el listenningSocket vuelva a esperar conexiones, necesitariamos volver a decirle que escuche, con listen();
	 *				En este ejemplo nos dedicamos unicamente a trabajar con el cliente y no escuchamos mas conexiones.
	 *
	 */
	struct sockaddr_in addr; // Esta estructura contendra los datos de la conexion del cliente. IP, puerto, etc.
	socklen_t addrlen = sizeof(addr);

	int socketCliente = accept(listenningSocket, (struct sockaddr *) &addr,
			&addrlen);

	/*
	 * 	Ya estamos listos para recibir paquetes de nuestro cliente...
	 *		 *
	 *	Cuando el cliente cierra la conexion, recieve_and_deserialize() devolvera 0.
	 */

	t_PackagePosta package;
	int status = 1;		// Estructura que manjea el status de los recieve.

	//t_PackageEnv packageEnvio;
	//packageEnvio.message = malloc(MAX_MESSAGE_SIZE);
	//char *serializedPackage;

	printf("Memoria conectada. Esperando Env��o de mensajes.\n");

	while (status) {
		status = recieve_and_deserialize(&package, socketCliente);

		interpretar_parametros(package.header,package.message);

		//fill_package(&packageEnvio, &username);

		// Ver el "Deserializando estructuras dinamicas" en el comentario de la funcion.
		if (status)
			printf("Memory says: %s.\n", package.message);

		//serializedPackage = serializarOperandos(&packageEnvio);
		//send(socketCliente, serializedPackage, packageEnvio.total_size, 0);
		//dispose_package(&serializedPackage);
	}

	printf("Cliente Desconectado.\n");
	/*
	 * 	Terminado el intercambio de paquetes, cerramos todas las conexiones y nos vamos a mirar Game of Thrones, que seguro nos vamos a divertir mas...
	 *
	 *
	 * 																					~ Divertido es Disney ~
	 *
	 */
	close(socketCliente);
	close(listenningSocket);

	/* See ya! */

	log_destroy(logger);
	config_destroy(config);

	return 0;
}

t_config* leer_config() {
	return config_create(CONFIG_PATH);
}

t_log* iniciar_logger(void) {

	return log_create(LOG_FILE_PATH, "LFS", 1, LOG_LEVEL_INFO);

}

void interpretar_parametros(int header, char* parametros){


	switch(header){

	case SELECT:
		lfs_select(parametros);
		break;
	case INSERT:
		lfs_insert(parametros);
		break;
	}
}

//Falta agregar funcionalidad de que debe buscar a la tabla correspondiente el valor y demas...
void lfs_select(char* parametros){

	t_log* logger_select = iniciar_logger();
	log_info(logger_select,"Estoy en la instruccion SELECT");

	char** parametros_separados = string_split(parametros, " ");
	char* nombre_tabla = malloc(strlen(parametros_separados[0]));
	strcpy(nombre_tabla,parametros_separados[0]);

	int key = atoi(parametros_separados[1]);

	log_info(logger_select,nombre_tabla);
	log_info(logger_select,string_itoa(key));

	free(nombre_tabla);//Aca hice el free por las dudas, pero creo que no deberia hacerlo si lo vamos a usar depues...
	log_destroy(logger_select);

}

void lfs_insert(char* parametros){ //Falta agregar funcionalidad, solo reconoce los parametros necesarios
	t_log* logger_insert = iniciar_logger();
	log_info(logger_insert,"Estoy en la instruccion INSERT");

	char** parametros_separados = string_split(parametros, " ");

	char* nombre_tabla = malloc(strlen(parametros_separados[0]));
	strcpy(nombre_tabla,parametros_separados[0]);

	int key = atoi(parametros_separados[1]);

	char* value = malloc(strlen(parametros_separados[2]));
	strcpy(value, parametros_separados[2]);

	unsigned long timestamp = atoi(parametros_separados[3]);

	log_info(logger_insert,nombre_tabla);
	log_info(logger_insert, string_itoa(key));
	log_info(logger_insert, value);
	log_info(logger_insert, string_itoa(timestamp));

}

//Preguntar que onda esta opcion, si pierdo la referencia al hacer malloc y devolverlo. Comparar con la otra funcion de abajo

/*char* obtener_nombre_tabla(char** parametros_separados){
	char* nombre_tabla = malloc(strlen(parametros_separados[0]));
	strcpy(nombre_tabla,parametros_separados[0]);
	return nombre_tabla;
}
*/

/*void obtener_nombre_tabla(char* nombre_tabla, char** parametros_separados){
	nombre_tabla = malloc(strlen(parametros_separados[0]));
	strcpy(nombre_tabla,parametros_separados[0]);
}*/
