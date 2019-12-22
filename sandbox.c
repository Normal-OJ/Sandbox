#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <pwd.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

const pid_t SANDBOX_UID = 1450;
const pid_t SANDBOX_GID = 1450;

unsigned long parse_long(char *str) {
    unsigned long x = 0;
    for (char *p = str; *p; p++) x = x * 10 + *p - '0';
    return x;
}

pid_t pid;
long time_limit_to_watch;
bool time_limit_exceeded_killed;

void *watcher_thread(void *arg) {
    sleep(time_limit_to_watch);
    time_limit_exceeded_killed = true;
    kill(pid, SIGKILL);
    return arg; // Avoid 'parameter set but not used' warning
}

int main(int argc, char **argv) {
    if (argc != 10 + 1) {
        fprintf(stderr, "Error: need 10 arguments\n");
        fprintf(stderr, "Usage: %s program file_stdin file_stdout file_stderr time_limit  memory_limit large_stack output_limit process_limit file_result\n", argv[0]);
        return 1;
    }

    if (getuid() != 0) {
        fprintf(stderr, "Error: need root privileges\n");
        return 1;
    }

    char *program = argv[1],
         *file_stdin = argv[2],
         *file_stdout = argv[3],
         *file_stderr = argv[4],
         *file_result = argv[10];
    long time_limit = parse_long(argv[5]),
         memory_limit = parse_long(argv[6]),
         large_stack = parse_long(argv[7]),
         output_limit = parse_long(argv[8]),
         process_limit = parse_long(argv[9]);

    time_limit_to_watch = time_limit + 1000;

#ifdef LOG
    printf("Program: %s\n", program);
    printf("Standard input file: %s\n", file_stdin);
    printf("Standard output file: %s\n", file_stdout);
    printf("Standard error file: %s\n", file_stderr);
    printf("Time limit (seconds): %lu + %lu\n", time_limit, time_limit_reserve);
    printf("Memory limit (kilobytes): %lu + %lu\n", memory_limit, memory_limit_reserve);
    printf("Output limit (bytes): %lu\n", output_limit);
    printf("Process limit: %lu\n", process_limit);
    printf("Result file: %s\n", file_result);
#endif

    pid = fork();
    if (pid > 0) {
        // Parent process

        FILE *fresult = fopen(file_result, "w");
        if (!fresult) {
            printf("Failed to open result file '%s'.", file_result);
            return -1;
        }

        if (time_limit) {
          pthread_t thread_id;
          pthread_create(&thread_id, NULL, &watcher_thread, NULL);
        }

        struct rusage usage;
        int status;
        if (wait4(pid, &status, 0, &usage) == -1) {
            fprintf(fresult, "RE\nwait4() = -1\n0\n0\n");
            return 0;
        }

        if (WIFEXITED(status)) {
            // Not signaled - maybe exited normally
            if (time_limit_exceeded_killed || usage.ru_utime.tv_sec > time_limit) {
                fprintf(fresult, "TLE\nWEXITSTATUS() = %d\n", WEXITSTATUS(status));
            }
            else if (usage.ru_maxrss > memory_limit) {
                fprintf(fresult, "MLE\nWEXITSTATUS() = %d\n", WEXITSTATUS(status));
            }
            else if (WEXITSTATUS(status) != 0) {
                fprintf(fresult, "RE\nWIFEXITED - WEXITSTATUS() = %d\n", WEXITSTATUS(status));
            } 
            else {
                fprintf(fresult, "Exited Normally\nWIFEXITED - WEXITSTATUS() = %d\n", WEXITSTATUS(status));
            }
        } else {
            // Signaled
            int sig = WTERMSIG(status);
            if (time_limit_exceeded_killed || usage.ru_utime.tv_sec > time_limit || sig == SIGXCPU) {
                fprintf(fresult, "TLE\nWEXITSTATUS() = %d, WTERMSIG() = %d (%s)\n", WEXITSTATUS(status), sig, strsignal(sig));
            } 
            else if (sig == SIGXFSZ) {
                fprintf(fresult, "OLE\nWEXITSTATUS() = %d, WTERMSIG() = %d (%s)\n", WEXITSTATUS(status), sig, strsignal(sig));
            }
            else if (usage.ru_maxrss > memory_limit) {
                fprintf(fresult, "MLE\nWEXITSTATUS() = %d, WTERMSIG() = %d (%s)\n", WEXITSTATUS(status), sig, strsignal(sig));
            } 
            else {
                fprintf(fresult, "RE\nWEXITSTATUS() = %d, WTERMSIG() = %d (%s)\n", WEXITSTATUS(status), sig, strsignal(sig));
            }
        }

#ifdef LOG
        printf("memory_usage = %ld\n", usage.ru_maxrss);
        if (time_limit_exceeded_killed) printf("cpu_usage = %ld\n", time_limit_to_watch * 1000000);
        else printf("cpu_usage = %ld\n", usage.ru_utime.tv_sec * 1000000 + usage.ru_utime.tv_usec);
#endif
        if (time_limit_exceeded_killed) fprintf(fresult, "%ld\n", time_limit_to_watch * 1000000 / 1000);
        else fprintf(fresult, "%ld\n", (usage.ru_utime.tv_sec * 1000000 + usage.ru_utime.tv_usec) / 1000);
        fprintf(fresult, "%ld\n", usage.ru_maxrss);

        fclose(fresult);
    } 
    else {
#ifdef LOG
        puts("Entered child process.");
#endif

        // Child process

        if (time_limit) {
            struct rlimit lim;
            lim.rlim_cur = time_limit / 1000 + 1;
            if(time_limit % 1000 > 800)lim.rlim_cur += 1;
            lim.rlim_max = lim.rlim_cur + 1;
            setrlimit(RLIMIT_CPU, &lim);
        }

        if (memory_limit) {
            struct rlimit lim;
            lim.rlim_cur = (memory_limit) * 1024 * 2;
            lim.rlim_max = lim.rlim_cur + 1024;
            setrlimit(RLIMIT_AS, &lim);
            
            if (large_stack) {
                setrlimit(RLIMIT_STACK, &lim);
            }
        }

        if (output_limit) {
            struct rlimit lim;
            lim.rlim_cur = output_limit;
            lim.rlim_max = output_limit;
            setrlimit(RLIMIT_FSIZE, &lim);
        }

        if (process_limit) {
            struct rlimit lim;
            lim.rlim_cur = process_limit + 1;
            lim.rlim_max = process_limit + 1;
            setrlimit(RLIMIT_NPROC, &lim);
        }

#ifdef LOG
        puts("Entering target program...");
#endif


        if(strlen(file_stdin)){
            int fd = open(file_stdin, O_RDONLY);
            if(fd < 0){
#ifdef LOG
                puts("Cannot open file_stdin...");
#endif
                return -1;
            }
            dup2(fd, STDIN_FILENO);
            close(fd);
        }

        if (strlen(file_stdout)){
            int fd = open(file_stdout, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
            if(fd < 0){
#ifdef LOG
                puts("Cannot open file_stdout...");
#endif
                return -1;
            }
            dup2(fd, STDOUT_FILENO);
            close(fd);
        }

        if (strlen(file_stderr)){
            int fd = open(file_stderr, O_WRONLY|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
            if(fd < 0){
#ifdef LOG
                puts("Cannot open file_stderr...");
#endif
                return -1;
            }
            dup2(fd, STDERR_FILENO);
            close(fd);
        }

        setegid(SANDBOX_GID);
        setuid(SANDBOX_UID);        
        execlp(program, program, NULL);
    }
    return 0;
}