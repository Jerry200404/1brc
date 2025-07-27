#  **1️⃣🐝🏎️ The One Billion Row Challenge**

**Challenge blog post:** https://www.morling.dev/blog/one-billion-row-challenge/

**Official challenge repository:** https://github.com/gunnarmorling/1brc

**My implementation blog post (in-depth write-up):** https://www.dannyvankooten.com/blog/2024/1brc/

## **Generate the measurements file (1 billion rows)**

```
gcc -o create-sample create-sample.c -lm
./create-sample 1000000000
```

This will generate a `measurements.txt` file containing **1 billion rows**, approximately **13 GB** in size:

```c
ls -lh measurements.txt
-rw-r--r-- 1 xx xx 13G Jul  4 19:32 measurements.txt
```

The generation process takes a minute or two, but you only need to do it once.

## **Run the challenge:**

First, compile the program:

```C
gcc -o last7 last7.c -pthread
```

Then, execute the benchmark:

```c
time ./last measurements.txt >/dev/null
```

Example output:

```c
real    0m1.363s
user    0m0.002s
sys     0m0.000s
```

## **Results**

My test environment: 32 vCPUs, AMD EPYC 9654 96-Core Processor, 60GB RAM.

The best result I’ve achieved so far is **1.363 seconds**. However, since this is a rented server with only 32 cores in use (and some resources shared with other workloads), the performance might not be entirely stable.

The current top score on the leaderboard is **1.535 seconds**.
 If anyone has access to the official benchmark setup (32-core AMD EPYC™ 7502P (Zen2), 128GB RAM), feel free to test it and see if this version can take the lead :)