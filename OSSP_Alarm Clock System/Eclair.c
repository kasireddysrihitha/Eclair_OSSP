#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <errno.h>
#include <ctype.h>

/* ============================================================
   FILES AND CONSTANTS
   ============================================================ */

#define FIFO_NAME "alarm_control_fifo"
#define DATA_FILE "alarms.dat"
#define LOG_FILE "alarm_history.txt"

#define INITIAL_CAPACITY 10

#define MAX_LABEL 64
#define MAX_TIME 32
#define MAX_SOUND 32
#define MAX_COMMAND 128


/* ============================================================
   ALARM STATUS
   ============================================================ */

typedef enum
{
    ALARM_ACTIVE,
    ALARM_RINGING,
    ALARM_COMPLETED,
    ALARM_CANCELLED,
    ALARM_MISSED
} AlarmStatus;


/* ============================================================
   REPEAT TYPE
   ============================================================ */

typedef enum
{
    REPEAT_ONCE,
    REPEAT_DAILY,
    REPEAT_WEEKDAYS,
    REPEAT_WEEKENDS
} RepeatType;


/* ============================================================
   ALARM STRUCTURE
   ============================================================ */

typedef struct
{
    int id;

    pid_t pid;
    pid_t ppid;
    pid_t pgid;

    time_t target_time;

    int delay;
    int remaining;

    char alarm_time[MAX_TIME];

    char label[MAX_LABEL];

    char sound[MAX_SOUND];

    RepeatType repeat;

    AlarmStatus status;

    int active;

    int snooze_count;

} Alarm;


/* ============================================================
   GLOBAL VARIABLES
   ============================================================ */

Alarm *alarms = NULL;

int alarm_count = 0;

int alarm_capacity = INITIAL_CAPACITY;

int next_alarm_id = 1;


/* Anonymous pipe */

int event_pipe[2];


/* Monitor thread */

pthread_t monitor_thread;


/* Synchronization */

pthread_mutex_t alarm_mutex =
    PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t alarm_condition =
    PTHREAD_COND_INITIALIZER;

sem_t notification_semaphore;


/* Program state */

volatile int program_running = 1;


/* ============================================================
   ANALYTICS
   ============================================================ */

int total_created = 0;

int total_triggered = 0;

int total_cancelled = 0;

int total_completed = 0;

int total_snoozed = 0;

int total_missed = 0;

int total_recurring = 0;


/* ============================================================
   CHILD ALARM ID
   ============================================================ */

int child_alarm_id = 0;


/* ============================================================
   FUNCTION DECLARATIONS
   ============================================================ */

void initialize_system(void);

void cleanup_system(void);

void *monitor_function(void *arg);

void alarm_signal_handler(int signal_number);

void alarm_process(
    int alarm_id,
    int delay
);

int create_alarm(
    time_t target,
    const char *display_time,
    const char *label,
    RepeatType repeat,
    const char *sound
);

int create_delay_alarm(
    int delay,
    const char *label,
    RepeatType repeat,
    const char *sound
);

int calculate_delay_from_time(
    int hour,
    int minute,
    const char *ampm
);

void create_time_alarm(void);

void view_all_alarms(void);

void view_alarm_details(void);

void cancel_alarm(void);

void modify_alarm(void);

void snooze_alarm(void);

void show_statistics(void);

void show_process_information(void);

void show_system_information(void);

void show_current_time(void);

void daily_schedule(void);

void live_clock(void);

void process_pipe_events(void);

void process_fifo_commands(void);

void save_alarms(void);

void load_alarms(void);

void log_message(
    const char *message
);

void ensure_alarm_capacity(void);

const char *get_status_name(
    AlarmStatus status
);

const char *get_repeat_name(
    RepeatType repeat
);

void print_alarm(
    const Alarm *a
);

void play_alarm_sound(
    const char *sound
);

void handle_recurring_alarm(
    int index
);

time_t calculate_next_occurrence(
    time_t old_time,
    RepeatType repeat
);

void check_missed_alarms(void);

void input_string(
    char *buffer,
    int size
);

RepeatType get_repeat_choice(void);


/* ============================================================
   LOGGING
   ============================================================ */

void log_message(
    const char *message
)
{
    int fd;

    fd = open(
        LOG_FILE,
        O_WRONLY | O_CREAT | O_APPEND,
        0644
    );

    if (fd == -1)
    {
        return;
    }

    write(
        fd,
        message,
        strlen(message)
    );

    close(fd);
}


/* ============================================================
   STATUS NAME
   ============================================================ */

const char *get_status_name(
    AlarmStatus status
)
{
    switch (status)
    {
        case ALARM_ACTIVE:
            return "ACTIVE";

        case ALARM_RINGING:
            return "RINGING";

        case ALARM_COMPLETED:
            return "COMPLETED";

        case ALARM_CANCELLED:
            return "CANCELLED";

        case ALARM_MISSED:
            return "MISSED";

        default:
            return "UNKNOWN";
    }
}


/* ============================================================
   REPEAT NAME
   ============================================================ */

const char *get_repeat_name(
    RepeatType repeat
)
{
    switch (repeat)
    {
        case REPEAT_ONCE:
            return "Once";

        case REPEAT_DAILY:
            return "Daily";

        case REPEAT_WEEKDAYS:
            return "Weekdays";

        case REPEAT_WEEKENDS:
            return "Weekends";

        default:
            return "Unknown";
    }
}


/* ============================================================
   DYNAMIC MEMORY
   ============================================================ */

void ensure_alarm_capacity(void)
{
    Alarm *temp;

    if (alarm_count < alarm_capacity)
    {
        return;
    }

    alarm_capacity *= 2;

    temp = realloc(
        alarms,
        alarm_capacity * sizeof(Alarm)
    );

    if (temp == NULL)
    {
        perror("realloc");

        exit(EXIT_FAILURE);
    }

    alarms = temp;
}


/* ============================================================
   INPUT STRING
   ============================================================ */

void input_string(
    char *buffer,
    int size
)
{
    int c;

    if (fgets(
            buffer,
            size,
            stdin
        ) == NULL)
    {
        buffer[0] = '\0';
        return;
    }

    buffer[
        strcspn(
            buffer,
            "\n"
        )
    ] = '\0';

    /*
       Remove remaining input if too long.
    */

    if (
        strlen(buffer) ==
        (size_t)(size - 1)
    )
    {
        while (
            (c = getchar()) != '\n' &&
            c != EOF
        )
            ;
    }
}


/* ============================================================
   SIGNAL HANDLER
   ============================================================ */

void alarm_signal_handler(
    int signal_number
)
{
    (void)signal_number;

    /*
       Only async-signal-safe operation:
       write().
    */

    write(
        event_pipe[1],
        &child_alarm_id,
        sizeof(child_alarm_id)
    );

    _exit(0);
}


/* ============================================================
   CHILD ALARM PROCESS
   ============================================================ */

void alarm_process(
    int alarm_id,
    int delay
)
{
    child_alarm_id = alarm_id;

    /*
       Create process group.
    */

    setpgid(
        0,
        0
    );

    /*
       Install signal handler.
    */

    signal(
        SIGALRM,
        alarm_signal_handler
    );

    /*
       Set Linux alarm timer.
    */

    alarm(
        (unsigned int)delay
    );

    /*
       Suspend process while waiting.
    */

    pause();

    _exit(0);
}


/* ============================================================
   CREATE ALARM
   ============================================================ */

int create_alarm(
    time_t target,
    const char *display_time,
    const char *label,
    RepeatType repeat,
    const char *sound
)
{
    pid_t pid;

    int id;

    int delay;


    delay = (int)difftime(
        target,
        time(NULL)
    );


    if (delay <= 0)
    {
        printf(
            "Cannot create an alarm in the past.\n"
        );

        return -1;
    }


    /*
       alarm() accepts unsigned int.
       Limit for practical project use.
    */

    if (
        (unsigned long)delay >
        4294967294UL
    )
    {
        printf(
            "Alarm time is too far in the future.\n"
        );

        return -1;
    }


    pthread_mutex_lock(
        &alarm_mutex
    );


    ensure_alarm_capacity();


    id = next_alarm_id++;


    pthread_mutex_unlock(
        &alarm_mutex
    );


    /*
       Create child.
    */

    pid = fork();


    if (pid == -1)
    {
        perror("fork");

        return -1;
    }


    /*
       CHILD
    */

    if (pid == 0)
    {
        close(
            event_pipe[0]
        );

        alarm_process(
            id,
            delay
        );

        _exit(0);
    }


    /*
       PARENT
    */

    pthread_mutex_lock(
        &alarm_mutex
    );


    alarms[alarm_count].id = id;

    alarms[alarm_count].pid = pid;

    alarms[alarm_count].ppid =
        getpid();

    alarms[alarm_count].pgid = pid;

    alarms[alarm_count].target_time =
        target;

    alarms[alarm_count].delay =
        delay;

    alarms[alarm_count].remaining =
        delay;


    snprintf(
        alarms[alarm_count].alarm_time,
        sizeof(
            alarms[alarm_count].alarm_time
        ),
        "%s",
        display_time
    );


    snprintf(
        alarms[alarm_count].label,
        sizeof(
            alarms[alarm_count].label
        ),
        "%s",
        label
    );


    snprintf(
        alarms[alarm_count].sound,
        sizeof(
            alarms[alarm_count].sound
        ),
        "%s",
        sound
    );


    alarms[alarm_count].repeat =
        repeat;

    alarms[alarm_count].status =
        ALARM_ACTIVE;

    alarms[alarm_count].active =
        1;

    alarms[alarm_count].snooze_count =
        0;


    if (
        repeat != REPEAT_ONCE
    )
    {
        total_recurring++;
    }


    alarm_count++;

    total_created++;


    pthread_cond_signal(
        &alarm_condition
    );


    pthread_mutex_unlock(
        &alarm_mutex
    );


    /*
       Set process group.
    */

    setpgid(
        pid,
        pid
    );


    /*
       Save persistent data.
    */

    save_alarms();


    /*
       Log.
    */

    {
        char log_text[512];

        snprintf(
            log_text,
            sizeof(log_text),
            "Alarm %d created | PID=%d | Time=%s | Label=%s | Repeat=%s\n",
            id,
            pid,
            display_time,
            label,
            get_repeat_name(repeat)
        );

        log_message(
            log_text
        );
    }


    printf("\n");
    printf(
        "========================================\n"
    );

    printf(
        "Alarm created successfully!\n"
    );

    printf(
        "Alarm ID : %d\n",
        id
    );

    printf(
        "PID      : %d\n",
        pid
    );

    printf(
        "Time     : %s\n",
        display_time
    );

    printf(
        "Label    : %s\n",
        label
    );

    printf(
        "Repeat   : %s\n",
        get_repeat_name(repeat)
    );

    printf(
        "Waiting  : %d seconds\n",
        delay
    );

    printf(
        "========================================\n"
    );


    return id;
}


/* ============================================================
   CREATE DELAY ALARM
   ============================================================ */

int create_delay_alarm(
    int delay,
    const char *label,
    RepeatType repeat,
    const char *sound
)
{
    time_t target;

    char display_time[MAX_TIME];

    struct tm *tm_info;


    if (delay <= 0)
    {
        printf(
            "Delay must be greater than zero.\n"
        );

        return -1;
    }


    target =
        time(NULL) + delay;


    tm_info =
        localtime(&target);


    strftime(
        display_time,
        sizeof(display_time),
        "%I:%M:%S %p",
        tm_info
    );


    return create_alarm(
        target,
        display_time,
        label,
        repeat,
        sound
    );
}


/* ============================================================
   CALCULATE SPECIFIC TIME
   ============================================================ */

int calculate_delay_from_time(
    int hour,
    int minute,
    const char *ampm
)
{
    time_t now;

    struct tm current_time;

    struct tm target_time;

    int target_hour;

    time_t target;

    int delay;


    if (
        hour < 1 ||
        hour > 12
    )
    {
        return -1;
    }


    if (
        minute < 0 ||
        minute > 59
    )
    {
        return -1;
    }


    /*
       Convert AM/PM.
    */

    if (
        strcasecmp(
            ampm,
            "AM"
        ) == 0
    )
    {
        if (hour == 12)
        {
            target_hour = 0;
        }
        else
        {
            target_hour = hour;
        }
    }

    else if (
        strcasecmp(
            ampm,
            "PM"
        ) == 0
    )
    {
        if (hour == 12)
        {
            target_hour = 12;
        }
        else
        {
            target_hour =
                hour + 12;
        }
    }

    else
    {
        return -1;
    }


    now = time(NULL);

    current_time =
        *localtime(&now);


    target_time =
        current_time;


    target_time.tm_hour =
        target_hour;

    target_time.tm_min =
        minute;

    target_time.tm_sec =
        0;


    target =
        mktime(
            &target_time
        );


    /*
       Already passed today:
       schedule tomorrow.
    */

    if (
        target <= now
    )
    {
        target_time.tm_mday++;

        target =
            mktime(
                &target_time
            );
    }


    delay =
        (int)difftime(
            target,
            now
        );


    return delay;
}


/* ============================================================
   REPEAT CHOICE
   ============================================================ */

RepeatType get_repeat_choice(void)
{
    int choice;


    printf("\n");
    printf(
        "Repeat Options:\n"
    );

    printf(
        "1. Once\n"
    );

    printf(
        "2. Daily\n"
    );

    printf(
        "3. Weekdays\n"
    );

    printf(
        "4. Weekends\n"
    );

    printf(
        "Enter choice: "
    );


    if (
        scanf(
            "%d",
            &choice
        ) != 1
    )
    {
        while (
            getchar() != '\n'
        )
            ;

        return REPEAT_ONCE;
    }


    switch (choice)
    {
        case 2:
            return REPEAT_DAILY;

        case 3:
            return REPEAT_WEEKDAYS;

        case 4:
            return REPEAT_WEEKENDS;

        default:
            return REPEAT_ONCE;
    }
}


/* ============================================================
   SET SPECIFIC TIME ALARM
   ============================================================ */

void create_time_alarm(void)
{
    int hour;

    int minute;

    char ampm[4];

    char label[MAX_LABEL];

    char sound[MAX_SOUND];

    int delay;

    time_t target;

    char display_time[MAX_TIME];

    RepeatType repeat;


    printf("\n");
    printf(
        "========================================\n"
    );

    printf(
        "       SET ALARM FOR SPECIFIC TIME\n"
    );

    printf(
        "========================================\n"
    );


    /*
       Hour.
    */

    printf(
        "Enter hour (1-12): "
    );


    if (
        scanf(
            "%d",
            &hour
        ) != 1
    )
    {
        printf(
            "Invalid hour.\n"
        );

        while (
            getchar() != '\n'
        )
            ;

        return;
    }


    /*
       Minute.
    */

    printf(
        "Enter minute (0-59): "
    );


    if (
        scanf(
            "%d",
            &minute
        ) != 1
    )
    {
        printf(
            "Invalid minute.\n"
        );

        while (
            getchar() != '\n'
        )
            ;

        return;
    }


    /*
       AM/PM.
    */

    printf(
        "Enter AM or PM: "
    );


    scanf(
        "%3s",
        ampm
    );


    if (
        strcasecmp(
            ampm,
            "AM"
        ) != 0 &&
        strcasecmp(
            ampm,
            "PM"
        ) != 0
    )
    {
        printf(
            "Invalid AM/PM.\n"
        );

        return;
    }


    /*
       Calculate delay.
    */

    delay =
        calculate_delay_from_time(
            hour,
            minute,
            ampm
        );


    if (delay < 0)
    {
        printf(
            "Invalid time.\n"
        );

        return;
    }


    /*
       Get label.
    */

    while (
        getchar() != '\n'
    )
        ;


    printf(
        "Enter alarm label: "
    );


    input_string(
        label,
        sizeof(label)
    );


    if (
        strlen(label) == 0
    )
    {
        strcpy(
            label,
            "General Alarm"
        );
    }


    /*
       Sound.
    */

    printf(
        "Sound name (default): "
    );


    input_string(
        sound,
        sizeof(sound)
    );


    if (
        strlen(sound) == 0
    )
    {
        strcpy(
            sound,
            "default"
        );
    }


    /*
       Repeat.
    */

    repeat =
        get_repeat_choice();


    /*
       Target time.
    */

    target =
        time(NULL) + delay;


    /*
       Display target.
    */

    snprintf(
        display_time,
        sizeof(display_time),
        "%02d:%02d %s",
        hour,
        minute,
        ampm
    );


    create_alarm(
        target,
        display_time,
        label,
        repeat,
        sound
    );
}


/* ============================================================
   PRINT ALARM
   ============================================================ */

void print_alarm(
    const Alarm *a
)
{
    printf("\n");
    printf(
        "----------------------------------------\n"
    );

    printf(
        "Alarm ID    : %d\n",
        a->id
    );

    printf(
        "PID         : %d\n",
        a->pid
    );

    printf(
        "PPID        : %d\n",
        a->ppid
    );

    printf(
        "PGID        : %d\n",
        a->pgid
    );

    printf(
        "Time        : %s\n",
        a->alarm_time
    );

    printf(
        "Label       : %s\n",
        a->label
    );

    printf(
        "Repeat      : %s\n",
        get_repeat_name(
            a->repeat
        )
    );

    printf(
        "Sound       : %s\n",
        a->sound
    );

    printf(
        "Remaining   : %d seconds\n",
        a->remaining
    );

    printf(
        "Snoozes     : %d\n",
        a->snooze_count
    );

    printf(
        "Status      : %s\n",
        get_status_name(
            a->status
        )
    );

    printf(
        "----------------------------------------\n"
    );
}


/* ============================================================
   VIEW ALL ALARMS
   ============================================================ */

void view_all_alarms(void)
{
    pthread_mutex_lock(
        &alarm_mutex
    );


    if (
        alarm_count == 0
    )
    {
        printf(
            "\nNo alarms available.\n"
        );

        pthread_mutex_unlock(
            &alarm_mutex
        );

        return;
    }


    printf("\n");
    printf(
        "========================================\n"
    );

    printf(
        "              ALL ALARMS\n"
    );

    printf(
        "========================================\n"
    );


    for (
        int i = 0;
        i < alarm_count;
        i++
    )
    {
        print_alarm(
            &alarms[i]
        );
    }


    pthread_mutex_unlock(
        &alarm_mutex
    );
}


/* ============================================================
   VIEW ALARM DETAILS
   ============================================================ */

void view_alarm_details(void)
{
    int id;

    int found = 0;


    printf(
        "\nEnter Alarm ID: "
    );


    if (
        scanf(
            "%d",
            &id
        ) != 1
    )
    {
        printf(
            "Invalid ID.\n"
        );

        while (
            getchar() != '\n'
        )
            ;

        return;
    }


    pthread_mutex_lock(
        &alarm_mutex
    );


    for (
        int i = 0;
        i < alarm_count;
        i++
    )
    {
        if (
            alarms[i].id == id
        )
        {
            print_alarm(
                &alarms[i]
            );

            found = 1;

            break;
        }
    }


    pthread_mutex_unlock(
        &alarm_mutex
    );


    if (!found)
    {
        printf(
            "Alarm not found.\n"
        );
    }
}


/* ============================================================
   CANCEL ALARM
   ============================================================ */

void cancel_alarm(void)
{
    int id;

    int found = 0;


    printf(
        "\nEnter Alarm ID to cancel: "
    );


    if (
        scanf(
            "%d",
            &id
        ) != 1
    )
    {
        printf(
            "Invalid ID.\n"
        );

        while (
            getchar() != '\n'
        )
            ;

        return;
    }


    pthread_mutex_lock(
        &alarm_mutex
    );


    for (
        int i = 0;
        i < alarm_count;
        i++
    )
    {
        if (
            alarms[i].id == id &&
            alarms[i].active
        )
        {
            if (
                kill(
                    alarms[i].pid,
                    SIGTERM
                ) == 0
            )
            {
                alarms[i].status =
                    ALARM_CANCELLED;

                alarms[i].active = 0;

                total_cancelled++;

                found = 1;


                printf(
                    "\nAlarm %d cancelled.\n",
                    id
                );


                {
                    char log_text[256];

                    snprintf(
                        log_text,
                        sizeof(log_text),
                        "Alarm %d cancelled\n",
                        id
                    );

                    log_message(
                        log_text
                    );
                }
            }

            break;
        }
    }


    pthread_mutex_unlock(
        &alarm_mutex
    );


    save_alarms();


    if (!found)
    {
        printf(
            "Active alarm not found.\n"
        );
    }
}


/* ============================================================
   SNOOZE
   ============================================================ */

void snooze_alarm(void)
{
    int id;

    int minutes;

    int found = 0;

    char label[MAX_LABEL];

    char sound[MAX_SOUND];

    RepeatType repeat;


    printf(
        "\nEnter Alarm ID to snooze: "
    );


    if (
        scanf(
            "%d",
            &id
        ) != 1
    )
    {
        printf(
            "Invalid ID.\n"
        );

        while (
            getchar() != '\n'
        )
            ;

        return;
    }


    printf(
        "Snooze duration in minutes: "
    );


    if (
        scanf(
            "%d",
            &minutes
        ) != 1
    )
    {
        printf(
            "Invalid duration.\n"
        );

        while (
            getchar() != '\n'
        )
            ;

        return;
    }


    if (minutes <= 0)
    {
        printf(
            "Duration must be greater than zero.\n"
        );

        return;
    }


    /*
       Save alarm information.
    */

    pthread_mutex_lock(
        &alarm_mutex
    );


    for (
        int i = 0;
        i < alarm_count;
        i++
    )
    {
        if (
            alarms[i].id == id &&
            alarms[i].active
        )
        {
            snprintf(
                label,
                sizeof(label),
                "%s",
                alarms[i].label
            );

            snprintf(
                sound,
                sizeof(sound),
                "%s",
                alarms[i].sound
            );

            repeat =
                alarms[i].repeat;


            kill(
                alarms[i].pid,
                SIGTERM
            );


            alarms[i].active = 0;

            alarms[i].status =
                ALARM_CANCELLED;

            alarms[i].snooze_count++;

            total_snoozed++;

            found = 1;

            break;
        }
    }


    pthread_mutex_unlock(
        &alarm_mutex
    );


    if (!found)
    {
        printf(
            "Active alarm not found.\n"
        );

        return;
    }


    /*
       Snoozed alarm becomes a new one-time alarm.
    */

    printf(
        "\nAlarm snoozed for %d minutes.\n",
        minutes
    );


    create_delay_alarm(
        minutes * 60,
        label,
        REPEAT_ONCE,
        sound
    );


    save_alarms();
}


/* ============================================================
   MODIFY ALARM
   ============================================================ */

void modify_alarm(void)
{
    int id;

    int minutes;

    int found = 0;

    char label[MAX_LABEL];

    char sound[MAX_SOUND];


    printf(
        "\nEnter Alarm ID to modify: "
    );


    if (
        scanf(
            "%d",
            &id
        ) != 1
    )
    {
        printf(
            "Invalid ID.\n"
        );

        while (
            getchar() != '\n'
        )
            ;

        return;
    }


    printf(
        "Enter new delay in seconds: "
    );


    if (
        scanf(
            "%d",
            &minutes
        ) != 1
    )
    {
        printf(
            "Invalid delay.\n"
        );

        while (
            getchar() != '\n'
        )
            ;

        return;
    }


    if (minutes <= 0)
    {
        printf(
            "Delay must be positive.\n"
        );

        return;
    }


    pthread_mutex_lock(
        &alarm_mutex
    );


    for (
        int i = 0;
        i < alarm_count;
        i++
    )
    {
        if (
            alarms[i].id == id &&
            alarms[i].active
        )
        {
            snprintf(
                label,
                sizeof(label),
                "%s",
                alarms[i].label
            );

            snprintf(
                sound,
                sizeof(sound),
                "%s",
                alarms[i].sound
            );


            kill(
                alarms[i].pid,
                SIGTERM
            );


            alarms[i].active = 0;

            alarms[i].status =
                ALARM_CANCELLED;

            total_cancelled++;

            found = 1;

            break;
        }
    }


    pthread_mutex_unlock(
        &alarm_mutex
    );


    if (!found)
    {
        printf(
            "Active alarm not found.\n"
        );

        return;
    }


    create_delay_alarm(
        minutes,
        label,
        REPEAT_ONCE,
        sound
    );


    save_alarms();
}


/* ============================================================
   PLAY SOUND
   ============================================================ */

void play_alarm_sound(
    const char *sound
)
{
    char command[512];


    /*
       Terminal bell works on many terminals.
    */

    printf(
        "\a\a\a"
    );

    fflush(stdout);


    /*
       Try paplay.
       
       Ubuntu systems commonly provide:
       /usr/share/sounds/freedesktop/stereo/
    */

    if (
        strcmp(
            sound,
            "default"
        ) == 0
    )
    {
        snprintf(
            command,
            sizeof(command),
            "if command -v paplay >/dev/null 2>&1 && [ -f /usr/share/sounds/freedesktop/stereo/alarm-clock-elapsed.oga ]; then paplay /usr/share/sounds/freedesktop/stereo/alarm-clock-elapsed.oga >/dev/null 2>&1 & elif command -v aplay >/dev/null 2>&1 && [ -f /usr/share/sounds/alsa/Front_Center.wav ]; then aplay /usr/share/sounds/alsa/Front_Center.wav >/dev/null 2>&1 & fi"
        );

        system(command);
    }
}


/* ============================================================
   CALCULATE NEXT RECURRING ALARM
   ============================================================ */

time_t calculate_next_occurrence(
    time_t old_time,
    RepeatType repeat
)
{
    struct tm tm_value;

    time_t next_time;


    tm_value =
        *localtime(&old_time);


    if (
        repeat == REPEAT_DAILY
    )
    {
        tm_value.tm_mday++;

        return mktime(
            &tm_value
        );
    }


    if (
        repeat == REPEAT_WEEKDAYS
    )
    {
        do
        {
            tm_value.tm_mday++;

            next_time =
                mktime(&tm_value);

            tm_value =
                *localtime(&next_time);

        } while (
            tm_value.tm_wday == 0 ||
            tm_value.tm_wday == 6
        );

        return next_time;
    }


    if (
        repeat == REPEAT_WEEKENDS
    )
    {
        do
        {
            tm_value.tm_mday++;

            next_time =
                mktime(&tm_value);

            tm_value =
                *localtime(&next_time);

        } while (
            tm_value.tm_wday != 0 &&
            tm_value.tm_wday != 6
        );

        return next_time;
    }


    return old_time;
}


/* ============================================================
   HANDLE RECURRING ALARM
   ============================================================ */

void handle_recurring_alarm(
    int index
)
{
    time_t next_time;

    char display_time[MAX_TIME];

    struct tm *tm_info;

    char label[MAX_LABEL];

    char sound[MAX_SOUND];

    RepeatType repeat;


    if (
        index < 0 ||
        index >= alarm_count
    )
    {
        return;
    }


    /*
       Copy required information.
    */

    snprintf(
        label,
        sizeof(label),
        "%s",
        alarms[index].label
    );

    snprintf(
        sound,
        sizeof(sound),
        "%s",
        alarms[index].sound
    );

    repeat =
        alarms[index].repeat;


    next_time =
        calculate_next_occurrence(
            alarms[index].target_time,
            repeat
        );


    /*
       Skip forward if necessary.
    */

    while (
        next_time <= time(NULL)
    )
    {
        next_time =
            calculate_next_occurrence(
                next_time,
                repeat
            );
    }


    tm_info =
        localtime(&next_time);


    strftime(
        display_time,
        sizeof(display_time),
        "%I:%M %p",
        tm_info
    );


    /*
       Create next occurrence.
    */

    create_alarm(
        next_time,
        display_time,
        label,
        repeat,
        sound
    );
}


/* ============================================================
   PROCESS ALARM TRIGGER EVENTS
   ============================================================ */

void process_pipe_events(void)
{
    int id;

    ssize_t bytes_read;


    while (1)
    {
        bytes_read =
            read(
                event_pipe[0],
                &id,
                sizeof(id)
            );


        if (
            bytes_read !=
            sizeof(id)
        )
        {
            break;
        }


        int recurring_index = -1;


        pthread_mutex_lock(
            &alarm_mutex
        );


        for (
            int i = 0;
            i < alarm_count;
            i++
        )
        {
            if (
                alarms[i].id == id
            )
            {
                alarms[i].status =
                    ALARM_RINGING;

                alarms[i].remaining =
                    0;

                alarms[i].active =
                    0;

                total_triggered++;

                total_completed++;


                if (
                    alarms[i].repeat !=
                    REPEAT_ONCE
                )
                {
                    recurring_index = i;
                }

                break;
            }
        }


        pthread_cond_broadcast(
            &alarm_condition
        );


        pthread_mutex_unlock(
            &alarm_mutex
        );


        /*
           Notification semaphore.
        */

        sem_post(
            &notification_semaphore
        );


        /*
           Display alarm.
        */

        printf("\n\n");

        printf(
            "****************************************\n"
        );

        printf(
            "             ALARM RINGING!\n"
        );

        printf(
            "****************************************\n"
        );


        pthread_mutex_lock(
            &alarm_mutex
        );


        for (
            int i = 0;
            i < alarm_count;
            i++
        )
        {
            if (
                alarms[i].id == id
            )
            {
                printf(
                    "Alarm ID : %d\n",
                    alarms[i].id
                );

                printf(
                    "Label    : %s\n",
                    alarms[i].label
                );

                printf(
                    "Time     : %s\n",
                    alarms[i].alarm_time
                );

                printf(
                    "Repeat   : %s\n",
                    get_repeat_name(
                        alarms[i].repeat
                    )
                );

                play_alarm_sound(
                    alarms[i].sound
                );

                break;
            }
        }


        pthread_mutex_unlock(
            &alarm_mutex
        );


        printf(
            "****************************************\n"
        );


        {
            char log_text[256];

            snprintf(
                log_text,
                sizeof(log_text),
                "Alarm %d triggered\n",
                id
            );

            log_message(
                log_text
            );
        }


        save_alarms();


        /*
           Create next recurring alarm.
        */

        if (
            recurring_index != -1
        )
        {
            handle_recurring_alarm(
                recurring_index
            );
        }
    }
}


/* ============================================================
   FIFO COMMANDS
   ============================================================ */

void process_fifo_commands(void)
{
    int fd;

    char buffer[MAX_COMMAND];


    fd = open(
        FIFO_NAME,
        O_RDONLY | O_NONBLOCK
    );


    if (fd == -1)
    {
        return;
    }


    while (1)
    {
        ssize_t n;

        n =
            read(
                fd,
                buffer,
                sizeof(buffer) - 1
            );


        if (n <= 0)
        {
            break;
        }


        buffer[n] =
            '\0';


        /*
           CANCEL ID
        */

        if (
            strncmp(
                buffer,
                "CANCEL",
                6
            ) == 0
        )
        {
            int id;


            if (
                sscanf(
                    buffer + 6,
                    "%d",
                    &id
                ) == 1
            )
            {
                pthread_mutex_lock(
                    &alarm_mutex
                );


                for (
                    int i = 0;
                    i < alarm_count;
                    i++
                )
                {
                    if (
                        alarms[i].id == id &&
                        alarms[i].active
                    )
                    {
                        kill(
                            alarms[i].pid,
                            SIGTERM
                        );


                        alarms[i].active =
                            0;

                        alarms[i].status =
                            ALARM_CANCELLED;

                        total_cancelled++;


                        printf(
                            "\nAlarm %d cancelled using FIFO.\n",
                            id
                        );

                        break;
                    }
                }


                pthread_mutex_unlock(
                    &alarm_mutex
                );


                save_alarms();
            }
        }
    }


    close(fd);
}


/* ============================================================
   MONITOR THREAD
   ============================================================ */

void *monitor_function(
    void *arg
)
{
    (void)arg;


    while (
        program_running
    )
    {
        /*
           Process alarm signals.
        */

        process_pipe_events();


        /*
           Process FIFO.
        */

        process_fifo_commands();


        /*
           Update remaining seconds.
        */

        pthread_mutex_lock(
            &alarm_mutex
        );


        for (
            int i = 0;
            i < alarm_count;
            i++
        )
        {
            if (
                alarms[i].active
            )
            {
                int remaining;


                remaining =
                    (int)difftime(
                        alarms[i].target_time,
                        time(NULL)
                    );


                if (
                    remaining < 0
                )
                {
                    remaining = 0;
                }


                alarms[i].remaining =
                    remaining;
            }
        }


        pthread_mutex_unlock(
            &alarm_mutex
        );


        /*
           Reap terminated child processes.
        */

        for (
            int i = 0;
            i < alarm_count;
            i++
        )
        {
            if (
                alarms[i].pid > 0
            )
            {
                int status;


                pid_t result =
                    waitpid(
                        alarms[i].pid,
                        &status,
                        WNOHANG
                    );


                if (
                    result ==
                    alarms[i].pid
                )
                {
                    alarms[i].pid =
                        -1;
                }
            }
        }


        /*
           Check missed alarms.
        */

        check_missed_alarms();


        /*
           Save current state.
        */

        save_alarms();


        sleep(1);
    }


    return NULL;
}


/* ============================================================
   MISSED ALARM DETECTION
   ============================================================ */

void check_missed_alarms(void)
{
    time_t now =
        time(NULL);


    pthread_mutex_lock(
        &alarm_mutex
    );


    for (
        int i = 0;
        i < alarm_count;
        i++
    )
    {
        if (
            alarms[i].active &&
            alarms[i].target_time < now
        )
        {
            /*
               Don't mark an alarm as missed if
               it has already triggered.
            */

            if (
                alarms[i].status ==
                ALARM_ACTIVE
            )
            {
                alarms[i].status =
                    ALARM_MISSED;

                alarms[i].active =
                    0;

                total_missed++;


                /*
                   Terminate its child if still alive.
                */

                if (
                    alarms[i].pid > 0
                )
                {
                    kill(
                        alarms[i].pid,
                        SIGTERM
                    );
                }


                printf(
                    "\n[Missed Alarm] ID %d - %s\n",
                    alarms[i].id,
                    alarms[i].label
                );


                {
                    char log_text[256];

                    snprintf(
                        log_text,
                        sizeof(log_text),
                        "Alarm %d marked as MISSED\n",
                        alarms[i].id
                    );

                    log_message(
                        log_text
                    );
                }
            }
        }
    }


    pthread_mutex_unlock(
        &alarm_mutex
    );
}


/* ============================================================
   PERSISTENT STORAGE
   ============================================================ */

void save_alarms(void)
{
    FILE *fp;


    /*
       Locking here is intentionally avoided
       because some callers already hold the mutex.
    */


    fp =
        fopen(
            DATA_FILE,
            "w"
        );


    if (fp == NULL)
    {
        return;
    }


    /*
       Store only alarms that can be useful
       after restart.
    */

    pthread_mutex_lock(
        &alarm_mutex
    );


    for (
        int i = 0;
        i < alarm_count;
        i++
    )
    {
        /*
           Save active alarms and missed alarms.
        */

        if (
            alarms[i].status ==
            ALARM_ACTIVE ||
            alarms[i].status ==
            ALARM_MISSED
        )
        {
            fprintf(
                fp,
                "%d|%lld|%s|%s|%d|%s\n",
                alarms[i].id,
                (long long)
                    alarms[i].target_time,
                alarms[i].alarm_time,
                alarms[i].label,
                (int)
                    alarms[i].repeat,
                alarms[i].sound
            );
        }
    }


    pthread_mutex_unlock(
        &alarm_mutex
    );


    fclose(fp);
}


/* ============================================================
   LOAD PERSISTENT ALARMS
   ============================================================ */

void load_alarms(void)
{
    FILE *fp;

    char line[512];

    time_t now =
        time(NULL);


    fp =
        fopen(
            DATA_FILE,
            "r"
        );


    if (fp == NULL)
    {
        return;
    }


    printf(
        "\nLoading saved alarms...\n"
    );


    while (
        fgets(
            line,
            sizeof(line),
            fp
        )
    )
    {
        int saved_id;

        long long saved_target;

        char saved_time[MAX_TIME];

        char saved_label[MAX_LABEL];

        int saved_repeat;

        char saved_sound[MAX_SOUND];


        char *token;


        /*
           ID
        */

        token =
            strtok(
                line,
                "|"
            );


        if (!token)
            continue;


        saved_id =
            atoi(token);


        /*
           Target.
        */

        token =
            strtok(
                NULL,
                "|"
            );


        if (!token)
            continue;


        saved_target =
            atoll(token);


        /*
           Display time.
        */

        token =
            strtok(
                NULL,
                "|"
            );


        if (!token)
            continue;


        snprintf(
            saved_time,
            sizeof(saved_time),
            "%s",
            token
        );


        /*
           Remove newline if present.
        */

        saved_time[
            strcspn(
                saved_time,
                "\n"
            )
        ] = '\0';


        /*
           Label.
        */

        token =
            strtok(
                NULL,
                "|"
            );


        if (!token)
            continue;


        snprintf(
            saved_label,
            sizeof(saved_label),
            "%s",
            token
        );


        /*
           Repeat.
        */

        token =
            strtok(
                NULL,
                "|"
            );


        if (!token)
            continue;


        saved_repeat =
            atoi(token);


        /*
           Sound.
        */

        token =
            strtok(
                NULL,
                "|\n"
            );


        if (!token)
        {
            strcpy(
                saved_sound,
                "default"
            );
        }
        else
        {
            snprintf(
                saved_sound,
                sizeof(saved_sound),
                "%s",
                token
            );
        }


        /*
           Avoid reusing old ID.
        */

        if (
            saved_id >= next_alarm_id
        )
        {
            next_alarm_id =
                saved_id + 1;
        }


        /*
           If the alarm is in the past.
        */

        if (
            (time_t)saved_target <= now
        )
        {
            RepeatType repeat =
                (RepeatType)
                    saved_repeat;


            if (
                repeat ==
                REPEAT_ONCE
            )
            {
                /*
                   Mark as missed.
                */

                ensure_alarm_capacity();


                alarms[alarm_count].id =
                    saved_id;

                alarms[alarm_count].pid =
                    -1;

                alarms[alarm_count].ppid =
                    getpid();

                alarms[alarm_count].pgid =
                    getpgrp();

                alarms[alarm_count].target_time =
                    (time_t)saved_target;

                alarms[alarm_count].delay =
                    0;

                alarms[alarm_count].remaining =
                    0;

                snprintf(
                    alarms[alarm_count].alarm_time,
                    sizeof(
                        alarms[alarm_count].alarm_time
                    ),
                    "%s",
                    saved_time
                );

                snprintf(
                    alarms[alarm_count].label,
                    sizeof(
                        alarms[alarm_count].label
                    ),
                    "%s",
                    saved_label
                );

                snprintf(
                    alarms[alarm_count].sound,
                    sizeof(
                        alarms[alarm_count].sound
                    ),
                    "%s",
                    saved_sound
                );

                alarms[alarm_count].repeat =
                    REPEAT_ONCE;

                alarms[alarm_count].status =
                    ALARM_MISSED;

                alarms[alarm_count].active =
                    0;

                alarms[alarm_count].snooze_count =
                    0;

                alarm_count++;

                total_missed++;


                printf(
                    "Missed alarm loaded: %s - %s\n",
                    saved_time,
                    saved_label
                );


                continue;
            }
            else
            {
                /*
                   Recurring alarm:
                   calculate next future occurrence.
                */

                time_t next_target =
                    (time_t)saved_target;


                while (
                    next_target <= now
                )
                {
                    next_target =
                        calculate_next_occurrence(
                            next_target,
                            repeat
                        );
                }


                saved_target =
                    (long long)
                        next_target;


                {
                    struct tm *tm_info;

                    tm_info =
                        localtime(
                            &next_target
                        );

                    strftime(
                        saved_time,
                        sizeof(saved_time),
                        "%I:%M %p",
                        tm_info
                    );
                }
            }
        }


        /*
           Calculate delay.
        */

        {
            int delay =
                (int)difftime(
                    (time_t)saved_target,
                    time(NULL)
                );


            if (delay <= 0)
            {
                continue;
            }


            /*
               Recreate the alarm process.
            */

            create_alarm(
                (time_t)saved_target,
                saved_time,
                saved_label,
                (RepeatType)
                    saved_repeat,
                saved_sound
            );
        }
    }


    fclose(fp);


    printf(
        "Saved alarms loaded successfully.\n"
    );
}


/* ============================================================
   DAILY SCHEDULE
   ============================================================ */

void daily_schedule(void)
{
    time_t now =
        time(NULL);

    struct tm today =
        *localtime(&now);


    int found = 0;


    printf("\n");
    printf(
        "========================================\n"
    );

    printf(
        "             TODAY'S SCHEDULE\n"
    );

    printf(
        "========================================\n"
    );


    pthread_mutex_lock(
        &alarm_mutex
    );


    /*
       Display active alarms.
    */

    for (
        int i = 0;
        i < alarm_count;
        i++
    )
    {
        struct tm alarm_tm =
            *localtime(
                &alarms[i].target_time
            );


        if (
            alarm_tm.tm_year ==
                today.tm_year &&
            alarm_tm.tm_mon ==
                today.tm_mon &&
            alarm_tm.tm_mday ==
                today.tm_mday
        )
        {
            found = 1;


            char time_buffer[32];


            strftime(
                time_buffer,
                sizeof(time_buffer),
                "%I:%M %p",
                &alarm_tm
            );


            printf(
                "%02d. %s | %-20s | %-10s | %s\n",
                alarms[i].id,
                time_buffer,
                alarms[i].label,
                get_repeat_name(
                    alarms[i].repeat
                ),
                get_status_name(
                    alarms[i].status
                )
            );
        }
    }


    pthread_mutex_unlock(
        &alarm_mutex
    );


    if (!found)
    {
        printf(
            "No alarms scheduled for today.\n"
        );
    }


    printf(
        "========================================\n"
    );
}


/* ============================================================
   ANALYTICS
   ============================================================ */

void show_statistics(void)
{
    int active = 0;

    int recurring = 0;


    pthread_mutex_lock(
        &alarm_mutex
    );


    for (
        int i = 0;
        i < alarm_count;
        i++
    )
    {
        if (
            alarms[i].active
        )
        {
            active++;
        }


        if (
            alarms[i].repeat !=
            REPEAT_ONCE
        )
        {
            recurring++;
        }
    }


    printf("\n");
    printf(
        "========================================\n"
    );

    printf(
        "            ALARM ANALYTICS\n"
    );

    printf(
        "========================================\n"
    );


    printf(
        "Total Created       : %d\n",
        total_created
    );

    printf(
        "Triggered           : %d\n",
        total_triggered
    );

    printf(
        "Cancelled           : %d\n",
        total_cancelled
    );

    printf(
        "Completed           : %d\n",
        total_completed
    );

    printf(
        "Snoozed             : %d\n",
        total_snoozed
    );

    printf(
        "Missed              : %d\n",
        total_missed
    );

    printf(
        "Recurring Alarms    : %d\n",
        recurring
    );

    printf(
        "Currently Active    : %d\n",
        active
    );

    printf(
        "Total Stored        : %d\n",
        alarm_count
    );


    if (
        total_created > 0
    )
    {
        double success_rate =
            (
                (double)total_triggered /
                total_created
            ) * 100.0;


        printf(
            "Trigger Rate        : %.2f%%\n",
            success_rate
        );
    }


    printf(
        "========================================\n"
    );


    pthread_mutex_unlock(
        &alarm_mutex
    );
}


/* ============================================================
   PROCESS INFORMATION
   ============================================================ */

void show_process_information(void)
{
    printf("\n");
    printf(
        "========================================\n"
    );

    printf(
        "          PROCESS INFORMATION\n"
    );

    printf(
        "========================================\n"
    );


    printf(
        "Main PID : %d\n",
        getpid()
    );

    printf(
        "Parent PID: %d\n",
        getppid()
    );

    printf(
        "Process Group: %d\n",
        getpgrp()
    );


    pthread_mutex_lock(
        &alarm_mutex
    );


    for (
        int i = 0;
        i < alarm_count;
        i++
    )
    {
        printf(
            "Alarm %d -> PID=%d PPID=%d PGID=%d Status=%s\n",
            alarms[i].id,
            alarms[i].pid,
            alarms[i].ppid,
            alarms[i].pgid,
            get_status_name(
                alarms[i].status
            )
        );
    }


    pthread_mutex_unlock(
        &alarm_mutex
    );
}


/* ============================================================
   SYSTEM INFORMATION
   ============================================================ */

void show_system_information(void)
{
    printf("\n");
    printf(
        "========================================\n"
    );

    printf(
        "            LINUX SYSTEM INFO\n"
    );

    printf(
        "========================================\n"
    );


    printf(
        "PID             : %d\n",
        getpid()
    );

    printf(
        "UID             : %d\n",
        getuid()
    );

    printf(
        "GID             : %d\n",
        getgid()
    );

    printf(
        "Page Size       : %ld bytes\n",
        sysconf(_SC_PAGESIZE)
    );

    printf(
        "CPU Cores       : %ld\n",
        sysconf(
            _SC_NPROCESSORS_ONLN
        )
    );

    printf(
        "POSIX Version   : %ld\n",
        sysconf(_SC_VERSION)
    );


    printf(
        "Data File       : %s\n",
        DATA_FILE
    );

    printf(
        "Log File        : %s\n",
        LOG_FILE
    );

    printf(
        "FIFO            : %s\n",
        FIFO_NAME
    );


    printf(
        "========================================\n"
    );
}


/* ============================================================
   CURRENT TIME
   ============================================================ */

void show_current_time(void)
{
    time_t now =
        time(NULL);

    struct tm *tm_info;

    char buffer[64];


    tm_info =
        localtime(&now);


    strftime(
        buffer,
        sizeof(buffer),
        "%A, %d %B %Y - %I:%M:%S %p",
        tm_info
    );


    printf(
        "\nCurrent Time: %s\n",
        buffer
    );
}


/* ============================================================
   LIVE CLOCK
   ============================================================ */

void live_clock(void)
{
    printf("\n");
    printf(
        "========================================\n"
    );

    printf(
        "              LIVE CLOCK\n"
    );

    printf(
        "========================================\n"
    );

    printf(
        "Press Ctrl+C to stop live mode.\n\n"
    );


    while (1)
    {
        time_t now =
            time(NULL);

        struct tm *tm_info;

        char buffer[64];


        tm_info =
            localtime(&now);


        strftime(
            buffer,
            sizeof(buffer),
            "%I:%M:%S %p",
            tm_info
        );


        printf(
            "\rCurrent Time: %s",
            buffer
        );


        pthread_mutex_lock(
            &alarm_mutex
        );


        for (
            int i = 0;
            i < alarm_count;
            i++
        )
        {
            if (
                alarms[i].active
            )
            {
                printf(
                    " | Alarm %d: %d sec",
                    alarms[i].id,
                    alarms[i].remaining
                );
            }
        }


        pthread_mutex_unlock(
            &alarm_mutex
        );


        fflush(stdout);


        sleep(1);
    }
}


/* ============================================================
   INITIALIZE SYSTEM
   ============================================================ */

void initialize_system(void)
{
    /*
       Dynamic memory.
    */

    alarms =
        malloc(
            INITIAL_CAPACITY *
            sizeof(Alarm)
        );


    if (
        alarms == NULL
    )
    {
        perror("malloc");

        exit(EXIT_FAILURE);
    }


    alarm_capacity =
        INITIAL_CAPACITY;


    /*
       Anonymous pipe.
    */

    if (
        pipe(event_pipe) == -1
    )
    {
        perror("pipe");

        free(alarms);

        exit(EXIT_FAILURE);
    }


    /*
       Non-blocking pipe read.
    */

    {
        int flags =
            fcntl(
                event_pipe[0],
                F_GETFL,
                0
            );


        fcntl(
            event_pipe[0],
            F_SETFL,
            flags | O_NONBLOCK
        );
    }


    /*
       Semaphore.
    */

    if (
        sem_init(
            &notification_semaphore,
            0,
            0
        ) == -1
    )
    {
        perror("sem_init");

        exit(EXIT_FAILURE);
    }


    /*
       FIFO.
    */

    unlink(
        FIFO_NAME
    );


    if (
        mkfifo(
            FIFO_NAME,
            0666
        ) == -1
    )
    {
        if (
            errno != EEXIST
        )
        {
            perror("mkfifo");
        }
    }


    /*
       Load persistent alarms.
    */

    load_alarms();


    /*
       Start monitor thread.
    */

    if (
        pthread_create(
            &monitor_thread,
            NULL,
            monitor_function,
            NULL
        ) != 0
    )
    {
        perror("pthread_create");

        exit(EXIT_FAILURE);
    }


    log_message(
        "Alarm Clock System started\n"
    );
}


/* ============================================================
   CLEANUP
   ============================================================ */

void cleanup_system(void)
{
    program_running = 0;


    /*
       Stop monitor thread.
    */

    pthread_join(
        monitor_thread,
        NULL
    );


    /*
       Save alarms.
    */

    save_alarms();


    /*
       Kill active children.
    */

    pthread_mutex_lock(
        &alarm_mutex
    );


    for (
        int i = 0;
        i < alarm_count;
        i++
    )
    {
        if (
            alarms[i].active &&
            alarms[i].pid > 0
        )
        {
            kill(
                alarms[i].pid,
                SIGTERM
            );
        }
    }


    pthread_mutex_unlock(
        &alarm_mutex
    );


    /*
       Reap children.
    */

    for (
        int i = 0;
        i < alarm_count;
        i++
    )
    {
        if (
            alarms[i].pid > 0
        )
        {
            waitpid(
                alarms[i].pid,
                NULL,
                0
            );
        }
    }


    /*
       Close pipe.
    */

    close(
        event_pipe[0]
    );

    close(
        event_pipe[1]
    );


    /*
       Synchronization cleanup.
    */

    pthread_mutex_destroy(
        &alarm_mutex
    );

    pthread_cond_destroy(
        &alarm_condition
    );

    sem_destroy(
        &notification_semaphore
    );


    /*
       Free memory.
    */

    free(
        alarms
    );

    alarms = NULL;


    /*
       Remove FIFO.
    */

    unlink(
        FIFO_NAME
    );


    log_message(
        "Alarm Clock System stopped\n"
    );
}


/* ============================================================
   MAIN
   ============================================================ */

int main(void)
{
    int choice;


    initialize_system();


    printf("\n");
    printf(
        "============================================\n"
    );

    printf(
        "          LINUX ALARM CLOCK SYSTEM\n"
    );

    printf(
        "============================================\n"
    );

    printf(
        "Advanced OS Alarm Management System\n"
    );

    printf(
        "============================================\n"
    );


    while (
        program_running
    )
    {
        printf("\n");
        printf(
            "========================================\n"
        );

        printf(
            "              MAIN MENU\n"
        );

        printf(
            "========================================\n"
        );


        printf(
            "1.  Set Alarm by Delay\n"
        );

        printf(
            "2.  Set Alarm for Specific Time\n"
        );

        printf(
            "3.  View All Alarms\n"
        );

        printf(
            "4.  View Alarm Details\n"
        );

        printf(
            "5.  Cancel Alarm\n"
        );

        printf(
            "6.  Modify Alarm\n"
        );

        printf(
            "7.  Snooze Alarm\n"
        );

        printf(
            "8.  Alarm Analytics\n"
        );

        printf(
            "9.  Process Information\n"
        );

        printf(
            "10. System Information\n"
        );

        printf(
            "11. Current Time\n"
        );

        printf(
            "12. Live Clock\n"
        );

        printf(
            "13. Today's Daily Schedule\n"
        );

        printf(
            "14. Exit\n"
        );


        printf(
            "========================================\n"
        );

        printf(
            "Enter your choice: "
        );


        if (
            scanf(
                "%d",
                &choice
            ) != 1
        )
        {
            printf(
                "Invalid input.\n"
            );

            while (
                getchar() != '\n'
            )
                ;

            continue;
        }


        switch (choice)
        {
            /* --------------------------------
               DELAY ALARM
               -------------------------------- */

            case 1:
            {
                int delay;

                char label[MAX_LABEL];

                char sound[MAX_SOUND];


                printf(
                    "\nEnter delay in seconds: "
                );


                if (
                    scanf(
                        "%d",
                        &delay
                    ) != 1
                )
                {
                    printf(
                        "Invalid delay.\n"
                    );

                    while (
                        getchar() != '\n'
                    )
                        ;

                    break;
                }


                if (delay <= 0)
                {
                    printf(
                        "Delay must be positive.\n"
                    );

                    break;
                }


                while (
                    getchar() != '\n'
                )
                    ;


                printf(
                    "Enter alarm label: "
                );


                input_string(
                    label,
                    sizeof(label)
                );


                if (
                    strlen(label) == 0
                )
                {
                    strcpy(
                        label,
                        "General Alarm"
                    );
                }


                printf(
                    "Sound name (default): "
                );


                input_string(
                    sound,
                    sizeof(sound)
                );


                if (
                    strlen(sound) == 0
                )
                {
                    strcpy(
                        sound,
                        "default"
                    );
                }


                create_delay_alarm(
                    delay,
                    label,
                    REPEAT_ONCE,
                    sound
                );


                break;
            }


            /* --------------------------