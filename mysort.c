#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <stdbool.h>

#define USE "./mysort <input file> <output file> <number of threads>"
//since our VM memory is 4GB
#define MAX_MEMMORY 4000000000
#define BUFFER_SIZE 100 // one line
#define ERROR -1

typedef struct ttask {
    void *(*thread_task)(void *);
    void *args;
    struct ttask* next;
} ttasks_t;

typedef struct {
    bool is_close; 
    int num_threads;
    int num_busy; // busy thread
    pthread_t check_t;
    pthread_mutex_t busy_lock;  
    pthread_t* ids_t;
    pthread_cond_t ready; // condition variable
    pthread_mutex_t lock;  // lock

    ttasks_t* head;
    ttasks_t* tail;

} tpools_t;

char* prefix = "temp_";

long gettheFileSize(char* filename) {
    struct stat statbuf;
    stat(filename, &statbuf);
    return (long)statbuf.st_size;
}

FILE* open_out_file(char *outputFile) {
    FILE* fout;
    fout = fopen(outputFile, "w");
    if (fout == NULL) {
        fprintf(stderr, "fopen(%s) failed", outputFile);
        return NULL;
    }
    return fout;
}
FILE* open_in_file(char *inFile) {
    FILE* fin;
    fin = fopen(inFile, "r");
    if (fin == NULL) {
        fprintf(stderr, "fopen(%s) failed", inFile);
        return NULL;
    }
    return fin;
}

void* thread_task(void* args) {
    tpools_t* pools = (tpools_t*)args;
    ttasks_t* task = NULL;

    while (true) {
        pthread_mutex_lock(&pools->lock);
        while (!pools->head && !pools->is_close) {
            pthread_cond_wait(&pools->ready, &pools->lock);
        }
        if (pools->is_close) {
            pthread_mutex_unlock(&pools->lock);
            pthread_exit(NULL);
        }

        task = pools->head;
        pools->head = pools->head->next;
        pthread_mutex_unlock(&pools->lock);
        task->thread_task(task->args);
        free(task);
    }
    return NULL;
}

int create_thread_pool(tpools_t** pool, int num_threads) {
    tpools_t* pools = (tpools_t *) malloc(sizeof(tpools_t));
    pools->is_close = false;
    pools->num_threads = num_threads;
    pools->ids_t = (pthread_t *) malloc(sizeof(pthread_t) * num_threads);
    pools->head = NULL;

    if (pthread_mutex_init(&(pools->lock), NULL) != 0) return ERROR;
    if (pthread_cond_init(&(pools->ready), NULL) != 0) return ERROR;

    for (int i = 0; i < num_threads; i++) {
        pthread_create(&pools->ids_t[i], NULL, thread_task, (void*)pools);
    }
    (*pool) = pools;
    return 0;
}

void destory_thread_pool(tpools_t* pools) {
    ttasks_t * task;
    if (pools->is_close) return;
    pools->is_close = true;
    pthread_mutex_lock(&pools->lock);
    pthread_cond_broadcast(&pools->ready);
    pthread_mutex_unlock(&pools->lock);

    for (int i = 0; i < pools->num_threads; i++) {
        pthread_join(pools->ids_t[i], NULL);
    }
    free(pools->ids_t);
    while(pools->head) {
        task = pools->head;
        pools->head = (ttasks_t *) pools->head->next;
        free(task);
    }
    pthread_mutex_destroy(&pools->lock);
    pthread_cond_destroy(&pools->ready);
    free(pools);
}

int add_task_pool(tpools_t* pools, void *(*taskfn)(void *), void* args) {
    ttasks_t* task;
    if (!taskfn) return ERROR;
    task = (ttasks_t*) malloc(sizeof(ttasks_t));
    task->thread_task = taskfn;
    task->args = args;
    task->next = NULL;

    pthread_mutex_lock(&pools->lock);
    if (!pools->head) {
        pools->head = task;
    } else {
        pools->tail->next = task;
    }
    pools->tail = task;

    pthread_cond_signal(&pools->ready);
    pthread_mutex_unlock(&pools->lock);
    return 0;
}

void swap(int* a, int* b) {
    int t = *a;
    *a = *b;
    *b = t;
}

// Partition function to select a pivot element and rearrange the array
int partition(int arr[], int low, int high) {
    int pivot = arr[high]; // Choose the rightmost element as the pivot
    int i = (low - 1); // Index of smaller element

    for (int j = low; j < high; j++) {
        // If the current element is smaller than or equal to the pivot
        if (arr[j] <= pivot) {
            i++; // Increment the index of smaller element
            swap(&arr[i], &arr[j]);
        }
    }

    swap(&arr[i + 1], &arr[high]);
    return (i + 1);
}

// Quick Sort function
void quickSort(int arr[], int low, int high) {
    if (low < high) {
        int pi = partition(arr, low, high);

        // Recursively sort elements before and after partition
        quickSort(arr, low, pi - 1);
        quickSort(arr, pi + 1, high);
    }
}

// Function to print an array
void printArray(int arr[], int size) {
    for (int i = 0; i < size; i++) {
        printf("%d ", arr[i]);
    }
    printf("\n");
}

struct file_block {
    char* input;
    long fsize;
    int i;
    char* outfile;
    // FILE* fout;
    size_t start;
    // size_t end;
};
struct internal_block {
    char* input;
    long count;
    char*** buffer;
    long start;
};
void* check(void* args) {
    tpools_t* pools = (tpools_t*)args;
    while (true) {
        sleep(3);
        pthread_mutex_lock(&pools->lock);
        if (!pools->head) {
            pthread_mutex_unlock(&pools->lock);
            destory_thread_pool(pools);
            printf("destory the pools finish\n");
            break;
        }
        pthread_mutex_unlock(&pools->lock);
    }
    return NULL;
}

int check_thread(tpools_t* pools) {
    pools->check_t = (pthread_t) malloc(sizeof(pthread_t));
    pthread_create(&pools->check_t, NULL, check, (void*)pools);
    pthread_join(pools->check_t, NULL);
    return 0;
}

int cmp(const void *a, const void *b)
{
     return (strcmp(*(char**)a, *(char**)b));
}

void* internal_sort(void* args) {
    struct internal_block *block = (struct internal_block*) args;
    char** buffer = *(block->buffer);
    FILE* fin = open_in_file(block->input);
    fseek(fin, block->start * BUFFER_SIZE, SEEK_SET);
    // printf("running the thread of %ld, %ld\n", block->start, block->count);
    // Read the entries line by line
    for (long i = 0; i < block->count; i++) {
        long index = block->start + i;
        // printf("block->start + i: %ld, %ld, %ld\n", index, block->start, i);
        fread(&buffer[index][0], BUFFER_SIZE, 1, fin);
    }
    fclose(fin);
    return NULL;
}

void* external_sort(void* args) {
    FILE* fout;
    char name[8];
    char* prefix = "temp_";
    struct file_block *block = (struct file_block*) args;
    FILE* fin = open_in_file(block->input);
    
    snprintf(name, 8,  "%s%i", prefix, block->i);
    fout = open_out_file(name);
    fseek(fin, block->start, SEEK_SET);
    long count = block->fsize / BUFFER_SIZE;

    char** buffer;
    buffer = (char**) malloc(count * sizeof(char*));
    for (int i = 0; i < count; i++) {
        buffer[i] = (char*) malloc(sizeof(char) * BUFFER_SIZE);
    }
    // Allocate memory for the buffer
    // printf("running the thread of %d, %ld, %s, %ld\n", block->i, block->fsize, name, count);
    // Read the entries line by line
    for (int i = 0; i < count; i++) {
        fread(&buffer[i][0], BUFFER_SIZE, 1, fin);
    }
    fclose(fin);
    // printf("sort start\n");
    qsort(buffer, count, sizeof(char*), cmp);
    // printf("0: %s, 1: %s \n", buffer[0], buffer[1]);
    // printf("sort end\n");
    printf("fwrite: %ld\n", count);
    for (int i = 0; i < count; i++) {
        fwrite(&buffer[i][0], BUFFER_SIZE, 1, fout);
    }
    for (int i = 0; i < count; i++) {
        free(buffer[i]);
    }
    free(buffer);
    fclose(fout);
    return NULL;
}



bool isfinish(long* seek_num, int k) {
    bool isFin = true;
    for (int i = 0; i < k; i++) {
        if (seek_num[i] >= 0) {
            isFin = false;
            break;
        }
    }
    return isFin;
}
int write_num = 0;
void cmp_write (char** buffer, char** omin, long* seek_num, int k, FILE* fout, FILE** fin, long* fincount, char* outputFile) {
    char *min = *omin;
    // min string
    int min_n;
    for (int i = 0; i < k; i++) {
        if (seek_num[i] != -1) {
            min_n = i;
            strcpy(min, buffer[i]);
            break;
        }
    }

    for (int i = 0; i < k; i++) {
        if (seek_num[i] == -1) continue;
        int ret = strcmp((const char*) buffer[i], (const char*) min);
        if (ret < 0) {
            strcpy(min, buffer[i]);
            min_n = i;
        }
    }

    // printf("min_n: %d, seek_num[min_n]: %ld, fincount[min_n]: %ld \n", min_n, seek_num[min_n], fincount[min_n]);
    if (seek_num[min_n] != -1 && seek_num[min_n] < fincount[min_n]) {
        seek_num[min_n] += 1;
    } else {
        seek_num[min_n] = -1;
    }

    fwrite(min, BUFFER_SIZE, 1, fout);
    
    if (isfinish(seek_num, k)) return;
    if (seek_num[min_n] != -1) {
        if (fseek(fin[min_n], seek_num[min_n] * BUFFER_SIZE, SEEK_SET) == 0) {
            fread(&buffer[min_n][0], BUFFER_SIZE, 1, fin[min_n]);
        } else {
            printf("seek fail");
        }
    }
}



// k sorted path
void* merge_files(char* outputFile, int k, long sum, int numThreads) {
    FILE *fin[k];
    FILE *fout = open_out_file(outputFile);
    long fincount[k];
    long seek_num[k];
    char** buffer;

    for (int i = 0; i < k; i++) {
        char name[8];
        snprintf(name, 8,  "%s%i", prefix, i);
        fin[i] = open_in_file(name);
        fincount[i] = ceil(gettheFileSize(name) / BUFFER_SIZE);
        // fincount[i] = 10;
        seek_num[i] = 0;
    }

    buffer = (char**) malloc(k * sizeof(char*));
    for (int i = 0; i < k; i++) {
        buffer[i] = (char*) malloc(sizeof(char) * BUFFER_SIZE);
        fread(&buffer[i][0], BUFFER_SIZE, 1, fin[i]);
    }

    char *min = (char *)malloc(BUFFER_SIZE * sizeof(char));
    write_num = 0;

    for (int i = 0; i < sum; i++) {
        cmp_write(buffer, &min, seek_num, k, fout, fin, fincount, outputFile);
    }

    // free
    for (int i = 0; i < k; i++) {
        free(buffer[i]);
    }
    free(buffer);
    fclose(fout);
    return NULL;
}

struct MemoryBlock {
    int id;
    int size; // Size of the memory block in KB
    int isAllocated; // 1 if allocated, 0 if free
};

// Function to display the memory management menu
void displayMenu() {
    printf("\nMemory Management Menu:\n");
    printf("1. Allocate memory block\n");
    printf("2. Deallocate memory block\n");
    printf("3. Display memory status\n");
    printf("4. Exit\n");
}

// TODO implement external sort
void mysort(char* inputFile, char* outputFile, int numThreads)
{
    // file size - 64GB
    long fsize = gettheFileSize(inputFile);

    tpools_t * pools = NULL;
    if (create_thread_pool(&pools, numThreads) != 0) {
        return;
    }
    
    if (fsize <= MAX_MEMMORY) {
        struct internal_block *blocks = (struct internal_block *) malloc(sizeof(struct internal_block) * numThreads);
        long total_line = fsize / BUFFER_SIZE;
        char** buffer = (char**) malloc(total_line * sizeof(char*));
        for (long i = 0; i < total_line; i++) {
            buffer[i] = (char*) malloc(sizeof(char) * BUFFER_SIZE);
        }
        long num = total_line / numThreads;
        printf("(internal sort used) file size is: %ld, the number of reading per each thread: %ld \n", fsize, num);
        for (long i = 0; i < numThreads; i++) {
            blocks[i].input = inputFile;
            blocks[i].count = num;
            blocks[i].start = num * i;
            blocks[i].buffer = &buffer;
            add_task_pool(pools, internal_sort, (void*) &blocks[i]);
        }
        // check if the thread pools task is finished or not
        check_thread(pools);
        qsort(buffer, total_line, sizeof(char*), cmp);
        // printf("sort end, fwrite: %ld\n", total_line);
        FILE *fout = open_out_file(outputFile);
        for (long i = 0; i < total_line; i++) {
            // printf("block->start + i: %s\n", buffer[i]);
            fwrite(&buffer[i][0], BUFFER_SIZE, 1, fout);
        }
        for (long i = 0; i < total_line; i++) {
            free(buffer[i]);
        }
        fclose(fout);
    } else {
        // the max memory of each thread - 0.5GB
        long max_size_thread = MAX_MEMMORY / numThreads;
        // the number of read times
        int count = fsize / max_size_thread == 0 ? 1: ceil(fsize / max_size_thread);
        printf("(external sort) whole file size: %ld, each thread file size per time: %ld, the number of read time: %d \n", fsize, max_size_thread, count);
        // read and using internal qsort() to sort, and write to temprory file.
        struct file_block *blocks = (struct file_block *) malloc(sizeof(struct file_block) * count);
        for (int i = 0; i < count; i++) {
            blocks[i].input = inputFile;
            blocks[i].i = i;
            blocks[i].fsize = max_size_thread;
            blocks[i].start = i * max_size_thread;
            // blocks[i].end = blocks[i].start + frsize;
            add_task_pool(pools, external_sort, (void*) &blocks[i]);
        }
        // check if the thread pools task is finished or not
        check_thread(pools);
        // K-way merge
        printf("k-way merge: %ld\n", fsize / BUFFER_SIZE);
        merge_files(outputFile, count, fsize / BUFFER_SIZE, numThreads);
    }
}

int main(int argc, char** argv) {
    char* inputFile;
    char* outputFile;
    int numThreads;
    struct timeval start, end;
    double executionTime;

    if (argc != 4) {
        fprintf(stderr, USE);
        return 1;
    }

    // Fetch the arguments
    inputFile = argv[1];
    outputFile = argv[2];
    numThreads = atoi(argv[3]);

    // Execute sort and measure execution time
    gettimeofday(&start, NULL);
    mysort(inputFile, outputFile, numThreads);
    gettimeofday(&end, NULL);
    executionTime = ((double) end.tv_sec - (double) start.tv_sec)
            + ((double) end.tv_usec - (double) start.tv_usec) / 1000000.0;
    
    printf("input file given is: %s\n", inputFile);
    printf("File generated as output: %s\n", outputFile);
    printf("number of threads utilized: %d\n", numThreads);
    printf("time taken for execution: %lf\n", executionTime);

    return 0;
}