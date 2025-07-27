#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdbool.h>
#include <sys/wait.h>
#define MAX_DISTINCT_GROUPS 10000
#define MAX_GROUPBY_KEY_LENGTH 100
#define HASHMAP_CAPACITY 131072

#ifndef NTHREADS
#define NTHREADS 64
#endif

#define BUFSIZE ((1<<10)*16)

static size_t chunk_count;
static size_t chunk_size;
static atomic_uint chunk_selector;
static size_t sz;

struct Group {
  unsigned int count;
  int min;
  int max;
  long sum;
  long firstNameWord;
  long secondNameWord;
  const char *nameAddress;
  int name_length;
};

struct Result {
  unsigned int n;
  unsigned int map[HASHMAP_CAPACITY];
  struct Group groups[MAX_DISTINCT_GROUPS];
};

static const long MASK1[] = { 
    0xFFL, 0xFFFFL, 0xFFFFFFL, 0xFFFFFFFFL, 
    0xFFFFFFFFFFL, 0xFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFL, 
    0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL 
};
static const long MASK2[] = { 
    0x00L, 0x00L, 0x00L, 0x00L, 0x00L, 0x00L, 0x00L, 0x00L, 
    0xFFFFFFFFFFFFFFFFL 
};

static struct Group *find_result(long initial_word, long initial_delimiter_mask, long word_b, long delimiter_mask_b, 
                                const char **s_ptr, struct Result *result);
static const char *parse_number(int *dest, const char *s);
static struct Group *new_entry(struct Result *result, const char *name_address, unsigned int table_index, int name_length);
static inline unsigned int *hashmap_entry(struct Result *result, const struct Group *b);
static void result_to_str(char *dest, const struct Result *result, size_t max_size);
static void *process_chunk(void *_data);

static inline int min(int a, int b) { return a < b ? a : b; }
static inline int max(int a, int b) { return a > b ? a : b; }

static inline int cmp(const void *ptr_a, const void *ptr_b) {
    const struct Group *a = (const struct Group *)ptr_a;
    const struct Group *b = (const struct Group *)ptr_b;
    
    int min_len = a->name_length < b->name_length ? a->name_length : b->name_length;
    int result = memcmp(a->nameAddress, b->nameAddress, min_len);
    
    if (result == 0) {
        return a->name_length - b->name_length;
    }
    return result;
}

static inline unsigned int hash_to_index(long hash) {
    long hash_as_int = hash ^ (hash >> 33) ^ (hash >> 15);
    return (unsigned int)(hash_as_int & (HASHMAP_CAPACITY - 1));
}

static inline long find_delimiter(long word) {
    long input = word ^ 0x3B3B3B3B3B3B3B3BL;
    return (input - 0x0101010101010101L) & ~input & 0x8080808080808080L;
}

static inline const char *next_new_line(const char *prev, const char *limit) {
    while (prev < limit) {
        long current_word = *(long *)prev;
        long input = current_word ^ 0x0A0A0A0A0A0A0A0AL;
        long pos = (input - 0x0101010101010101L) & ~input & 0x8080808080808080L;
        if (pos != 0) {
            prev += __builtin_ctzll(pos) >> 3;
            return prev + 1;
        } else {
            prev += 8;
        }
    }
    return limit;
}

static const char *parse_number(int *dest, const char *s) {
    long word = *(long*)s;

    long signed_mask = (~word << 59) >> 63;
    int decimal_pos = __builtin_ctzll(~word & 0x10101000);
    
    long digits = ((word & ~(signed_mask & 0xFF)) << (28 - decimal_pos)) & 0x0F000F0F00UL;
    long abs_value = ((digits * 0x640a0001) >> 32) & 0x3FF; 
    
    *dest = (abs_value ^ signed_mask) - signed_mask;
    
    return s + (decimal_pos >> 3) + 4;
}

static struct Group *new_entry(struct Result *result, const char *name_address, unsigned int table_index, int name_length) {
    struct Group *r = &result->groups[result->n];
    result->map[table_index] = result->n;
    result->n++;

    r->firstNameWord = *(long *)name_address;
    r->secondNameWord = *(long *)(name_address + 8);
    int total_length = name_length + 1;
    if (total_length <= 8) {
        r->firstNameWord &= MASK1[total_length - 1];
        r->secondNameWord = 0;
    } else if (total_length < 16) {
        r->secondNameWord &= MASK1[total_length - 9];
    }
    r->nameAddress = name_address;
    r->name_length = name_length;

    r->count = 0;
    r->min = 999;
    r->max = -999;
    r->sum = 0;

    return r;
}

static struct Group *find_result(long initial_word, long initial_delimiter_mask, long word_b, long delimiter_mask_b, 
                                const char **s_ptr, struct Result *result) {
    struct Group *existing_group;
    long word = initial_word;
    long delimiter_mask = initial_delimiter_mask;
    long hash;
    const char *name_address = *s_ptr;
    long word2 = word_b;
    long delimiter_mask2 = delimiter_mask_b;

    if ((delimiter_mask | delimiter_mask2) != 0) {
        int letter_count1 = __builtin_ctzll(delimiter_mask) >> 3;
        int letter_count2 = __builtin_ctzll(delimiter_mask2) >> 3;
        long mask = MASK2[letter_count1];
        word = word & MASK1[letter_count1];
        word2 = mask & word2 & MASK1[letter_count2];
        hash = word ^ word2;
        unsigned int table_index = hash_to_index(hash);
        existing_group = &result->groups[result->map[table_index]];
        *s_ptr = name_address + letter_count1 + (letter_count2 & mask);
        
        if (result->map[table_index] != 0 && 
            existing_group->firstNameWord == word && 
            existing_group->secondNameWord == word2) {
            return existing_group;
        }
    } else {
        hash = word ^ word2;
        *s_ptr = name_address + 16;
        while (1) {
            word = *(long *)(*s_ptr);
            delimiter_mask = find_delimiter(word);
            if (delimiter_mask != 0) {
                int trailing_zeros = __builtin_ctzll(delimiter_mask);
                word = (word << (63 - trailing_zeros));
                *s_ptr += (trailing_zeros >> 3);
                hash ^= word;
                break;
            } else {
                *s_ptr += 8;
                hash ^= word;
            }
        }
    }

    int name_length = (int)(*s_ptr - name_address);
    unsigned int table_index = hash_to_index(hash);

    while (1) {
        if (result->map[table_index] == 0) {
            existing_group = new_entry(result, name_address, table_index, name_length);
            break;
        }
        
        existing_group = &result->groups[result->map[table_index]];
        
        if (existing_group->name_length == name_length && 
            memcmp(existing_group->nameAddress, name_address, name_length) == 0) {
            break;
        }
        
        table_index = (table_index + 31) & (HASHMAP_CAPACITY - 1);
    }

    return existing_group;
}

static void *process_chunk(void *_data) {
    char *data = (char *)_data;
    struct Result *result = malloc(sizeof(*result));
    if (!result) {
        perror("malloc error");
        exit(EXIT_FAILURE);
    }
    result->n = 0;

    memset(result->map, 0, HASHMAP_CAPACITY * sizeof(*result->map));
    memset(result->groups, 0, MAX_DISTINCT_GROUPS * sizeof(*result->groups));

    while (1) {
        const unsigned int chunk = atomic_fetch_add(&chunk_selector, 1);
        if (chunk >= chunk_count) {
            break;
        }
        size_t chunk_start = chunk * chunk_size;
        size_t chunk_end = chunk_start + chunk_size > sz ? sz : chunk_start + chunk_size;
        
        const char *segment_start = chunk_start > 0 ? next_new_line(&data[chunk_start], &data[chunk_end]) : &data[chunk_start];
        const char *segment_end = chunk_end >= sz ? &data[sz] : next_new_line(&data[chunk_end], &data[sz]);
        
        size_t dist = (segment_end - segment_start) / 3;
        const char *mid_point1 = next_new_line(segment_start + dist, segment_end);
        const char *mid_point2 = next_new_line(segment_start + dist + dist, segment_end);
        
        const char *s1 = segment_start;
        const char *s2 = mid_point1 < segment_end ? mid_point1 + 1 : segment_end;
        const char *s3 = mid_point2 < segment_end ? mid_point2 + 1 : segment_end;

        while (s1 < mid_point1 && s2 < mid_point2 && s3 < segment_end) {

            long word1_1 = *(long *)s1;
            long word1_2 = *(long *)s2;
            long word1_3 = *(long *)s3;

            long delimiter_mask1_1 = find_delimiter(word1_1);
            long delimiter_mask1_2 = find_delimiter(word1_2);
            long delimiter_mask1_3 = find_delimiter(word1_3);

            long word2_1 = *(long *)(s1 + 8);
            long word2_2 = *(long *)(s2 + 8);
            long word2_3 = *(long *)(s3 + 8);

            long delimiter_mask2_1 = find_delimiter(word2_1);
            long delimiter_mask2_2 = find_delimiter(word2_2);
            long delimiter_mask2_3 = find_delimiter(word2_3);

            struct Group *group1 = find_result(word1_1, delimiter_mask1_1, word2_1, delimiter_mask2_1, &s1, result);
            struct Group *group2 = find_result(word1_2, delimiter_mask1_2, word2_2, delimiter_mask2_2, &s2, result);
            struct Group *group3 = find_result(word1_3, delimiter_mask1_3, word2_3, delimiter_mask2_3, &s3, result);

            int temperature1, temperature2, temperature3;
            s1 = parse_number(&temperature1, s1);
            s2 = parse_number(&temperature2, s2);
            s3 = parse_number(&temperature3, s3);

            group1->count += 1;
            group2->count += 1;
            group3->count += 1;
            group1->min = min(group1->min, temperature1);
            group2->min = min(group2->min, temperature2);
            group3->min = min(group3->min, temperature3);
            
            group1->max = max(group1->max, temperature1);
            group2->max = max(group2->max, temperature2);
            group3->max = max(group3->max, temperature3);
            
            group1->sum += temperature1;
            group2->sum += temperature2;
            group3->sum += temperature3;
        }
        
        while (s1 < mid_point1) {
            long word1 = *(long *)s1;
            long delimiter_mask1 = find_delimiter(word1);
            long word2 = *(long *)(s1 + 8);
            long delimiter_mask2 = find_delimiter(word2);
            struct Group *group = find_result(word1, delimiter_mask1, word2, delimiter_mask2, &s1, result);
            int temperature;
            s1 = parse_number(&temperature, s1);
            
            group->count += 1;
            group->min = min(group->min, temperature);
            group->max = max(group->max, temperature);
            group->sum += temperature;
        }
        
        while (s2 < mid_point2) {
            long word1 = *(long *)s2;
            long delimiter_mask1 = find_delimiter(word1);
            long word2 = *(long *)(s2 + 8);
            long delimiter_mask2 = find_delimiter(word2);
            struct Group *group = find_result(word1, delimiter_mask1, word2, delimiter_mask2, &s2, result);
            int temperature;
            s2 = parse_number(&temperature, s2);
            
            group->count += 1;
            group->min = min(group->min, temperature);
            group->max = max(group->max, temperature);
            group->sum += temperature;
        }
        
        while (s3 < segment_end) {
            long word1 = *(long *)s3;
            long delimiter_mask1 = find_delimiter(word1);
            long word2 = *(long *)(s3 + 8);
            long delimiter_mask2 = find_delimiter(word2);
            struct Group *group = find_result(word1, delimiter_mask1, word2, delimiter_mask2, &s3, result);
            int temperature;
            s3 = parse_number(&temperature, s3);
            
            group->count += 1;
            group->min = min(group->min, temperature);
            group->max = max(group->max, temperature);
            group->sum += temperature;
        }
    }

    return (void *)result;
}

static void result_to_str(char *dest, const struct Result *result, size_t max_size) {
    size_t pos = 0;
    
    if (pos < max_size - 1) {
        dest[pos++] = '{';
    }

    for (unsigned int i = 0; i < result->n && pos < max_size - 200; i++) {
        char temp_name[MAX_GROUPBY_KEY_LENGTH];
        int copy_len = result->groups[i].name_length;
        if (copy_len >= MAX_GROUPBY_KEY_LENGTH) {
            copy_len = MAX_GROUPBY_KEY_LENGTH - 1;
        }
        memcpy(temp_name, result->groups[i].nameAddress, copy_len);
        temp_name[copy_len] = '\0';
        
        int n = snprintf(
            dest + pos, max_size - pos, "%s=%.1f/%.1f/%.1f%s", temp_name,
            (float)result->groups[i].min / 10.0,
            ((float)result->groups[i].sum / (float)result->groups[i].count) / 10.0,
            (float)result->groups[i].max / 10.0,
            i < (result->n - 1) ? ", " : "");

        if (n > 0 && (size_t)n < max_size - pos) {
            pos += n;
        } else {
            break;
        }
    }

    if (pos < max_size - 2) {
        dest[pos++] = '}';
        dest[pos++] = '\n';
    }
    if (pos < max_size) {
        dest[pos] = '\0';
    }
}

static inline unsigned int *hashmap_entry(struct Result *result, const struct Group *b) {
    long hash = b->firstNameWord ^ b->secondNameWord;
    
    if (b->name_length > 16) {
        const char *p = b->nameAddress + 16;
        int remaining = b->name_length - 16;
        while (remaining > 0) {
            long word = 0;
            int bytes_to_read = remaining > 8 ? 8 : remaining;
            memcpy(&word, p, bytes_to_read);
            hash ^= word;
            p += 8;
            remaining -= 8;
        }
    }
    
    unsigned int table_index = hash_to_index(hash);
    unsigned int *c = &result->map[table_index];
    
    while (*c > 0) {
        struct Group *existing = &result->groups[*c];
        
        if (existing->name_length == b->name_length && 
            memcmp(existing->nameAddress, b->nameAddress, b->name_length) == 0) {
            break;
        }
        
        table_index = (table_index + 31) & (HASHMAP_CAPACITY - 1);
        c = &result->map[table_index];
    }
    return c;
}


int main(int argc, char **argv) {
    char *file = "measurements.txt";
    if (argc > 1) {
        file = argv[1];
    }
    
    int fd = open(file, O_RDONLY);
    if (fd == -1) {
        perror("error opening file");
        exit(EXIT_FAILURE);
    }
    
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        perror("error getting file size");
        exit(EXIT_FAILURE);
    }
    
    sz = (size_t)sb.st_size;
    
    // set-up pipes for communication
    int pipefd[2];
    if (pipe(pipefd) != 0) {
        perror("pipe error");
        exit(EXIT_FAILURE);
    }
    
    pid_t pid = fork();
    if (pid == -1) {
        perror("fork error");
        exit(EXIT_FAILURE);
    }
    
    if (pid > 0) {
        // parent process: read results and output immediately
        close(pipefd[1]); // close write pipe
        close(fd); // parent doesn't need the file
        
        char buf[BUFSIZE];
        ssize_t bytes_read = read(pipefd[0], buf, BUFSIZE - 1);
        if (bytes_read == -1) {
            perror("read error");
        } else {
            buf[bytes_read] = '\0';
            printf("%s", buf);
        }
        close(pipefd[0]);
        exit(EXIT_SUCCESS); // parent exits immediately after output
    }
    
    // child process: do all the heavy work
    close(pipefd[0]); // close unused read pipe
    
    // mmap entire file into memory
    char *data = mmap(NULL, sz, PROT_READ, MAP_SHARED, fd, 0);
    if (data == MAP_FAILED) {
        perror("error mmapping file");
        exit(EXIT_FAILURE);
    }
    
    // distribute work among N worker threads
    chunk_size = 5 * 1024 * 1024; 
    chunk_count = (sz + chunk_size - 1) / chunk_size;
    
    atomic_store(&chunk_selector, 0);
    
    pthread_t workers[NTHREADS];
    for (unsigned int i = 0; i < NTHREADS; i++) {
        pthread_create(&workers[i], NULL, process_chunk, data);
    }
    
    // wait for all threads to finish
    struct Result *results[NTHREADS];
    for (unsigned int i = 0; i < NTHREADS; i++) {
        pthread_join(workers[i], (void *)&results[i]);
    }
    
    // merge results
    struct Result *result = results[0];
    for (unsigned int i = 1; i < NTHREADS; i++) {
        for (unsigned int j = 0; j < results[i]->n; j++) {
            struct Group *b = &results[i]->groups[j];
            unsigned int *hm_entry = hashmap_entry(result, b);
            unsigned int c = *hm_entry;
            if (c == 0) {
                c = result->n++;
                *hm_entry = c;
                result->groups[c].nameAddress = b->nameAddress;
                result->groups[c].name_length = b->name_length;
                result->groups[c].firstNameWord = b->firstNameWord;
                result->groups[c].secondNameWord = b->secondNameWord;
                result->groups[c].min = 999;
                result->groups[c].max = -999;
                result->groups[c].count = 0;
                result->groups[c].sum = 0;
            }
            result->groups[c].count += b->count;
            result->groups[c].sum += b->sum;
            result->groups[c].min = min(result->groups[c].min, b->min);
            result->groups[c].max = max(result->groups[c].max, b->max);
        }
    }
    
    // sort results alphabetically
    qsort(result->groups, (size_t)result->n, sizeof(struct Group), cmp);
    
    // prepare output string
    char *buf = malloc(1024 * 1024); // 1MB buffer
    if (!buf) {
        perror("malloc error for output buffer");
        exit(EXIT_FAILURE);
    }
    result_to_str(buf, result, 1024 * 1024);
    
    // write result to pipe
    size_t len = strlen(buf);
    if (write(pipefd[1], buf, len) == -1) {
        perror("write error");
    }
    
    // close write pipe
    close(pipefd[1]);
    free(buf);
    
    // clean-up (this happens in background after parent has exited)
    munmap(data, sz);
    close(fd);
    for (unsigned int i = 0; i < NTHREADS; i++) {
        free(results[i]);
    }
    
    return EXIT_SUCCESS;
}
// 64 Thread 2MB
// real    0m1.390s
// user    0m0.002s
// sys     0m0.000s
// 64 Thread 3MB
// real    0m1.335s
// user    0m0.002s
// sys     0m0.000s
// 64 Thread 4MB
// real    0m1.342s
// user    0m0.000s
// sys     0m0.001s
// 64 Thread 5MB
// real    0m1.334s
// user    0m0.001s
// sys     0m0.000s
// 64 Thread 6MB
// real    0m1.348s
// user    0m0.001s
// sys     0m0.000s
// 64 Thread 8MB
// real    0m1.368s
// user    0m0.001s
// sys     0m0.000s
// 32 Thread 2MB
// real    0m1.371s
// user    0m0.000s
// sys     0m0.002s
// 32 Thread 3MB
// real    0m1.364s
// user    0m0.000s
// sys     0m0.001s
// 32 Thread 4MB
// real    0m1.363s
// user    0m0.002s
// sys     0m0.000s
// 32 Thread 5MB
// real    0m1.393s
// user    0m0.001s
// sys     0m0.001s
// 32 Thread 6MB
// real    0m1.397s
// user    0m0.002s
// sys     0m0.000s
// 32 Thread 8MB
// real    0m1.370s
// user    0m0.002s
// sys     0m0.000s
