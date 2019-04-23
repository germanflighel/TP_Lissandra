#ifndef KERNEL_H_
#define KERNEL_H_

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <stdint.h>

#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>
#include <commons/collections/list.h>
#include <commons/string.h>


#define IP "127.0.0.1"
#define PUERTO "6667"

#define CONFIG_PATH "KernelSocket.config"
#define LOG_FILE_PATH "kernel_global.log"

t_log* iniciar_logger(void);
void abrir_config(t_config **);

#endif
