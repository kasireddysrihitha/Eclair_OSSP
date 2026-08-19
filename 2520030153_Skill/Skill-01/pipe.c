#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>

int main() {
    int pipefd[2];
    pid_t pid;
    char buffer[100];

    pipe(pipefd);

    pid = fork();

    if (pid > 0) {
        close(pipefd[0]);

        char message[] = "Hello from Parent Process!";
        write(pipefd[1], message, strlen(message) + 1);

        close(pipefd[1]);
    }
    else if (pid == 0) {
        close(pipefd[1]);

        read(pipefd[0], buffer, sizeof(buffer));
        printf("Child Process received: %s\n", buffer);

        close(pipefd[0]);
    }

    return 0;
}
