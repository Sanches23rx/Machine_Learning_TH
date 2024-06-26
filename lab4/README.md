# Exercise №4

## Цель работы

1.  Изучить возможности СУБД DuckDB для обработки и анализ больших данных

2.  Получить навыки применения DuckDB совместно с языком программирования R

3.  Получить навыки анализа метаинфомации о сетевом трафике

4.  Получить навыки применения облачных технологий хранения, подготовки и анализа данных: Yandex Object Storage, Rstudio Server.

## Исходные данные

1.  ОС Windows
2.  OpenSSH Client
3.  Apache Arrow
4.  RStudio Server
5.  Yandex Object Storage
6.  DuckDB

## Ход работы

### Обеспечение доступа к датасету arrow-datasets/tm_data.pqt

``` r
library(duckdb)
```

```         
Loading required package: DBI
```

``` r
library(dplyr)
```

```         
Attaching package: 'dplyr'

The following objects are masked from 'package:stats':

    filter, lag

The following objects are masked from 'package:base':

    intersect, setdiff, setequal, union
```

``` r
library(tidyverse)
```

```         
── Attaching core tidyverse packages ──────────────────────── tidyverse 2.0.0 ──
✔ forcats   1.0.0     ✔ readr     2.1.5
✔ ggplot2   3.4.4     ✔ stringr   1.5.1
✔ lubridate 1.9.3     ✔ tibble    3.2.1
✔ purrr     1.0.2     ✔ tidyr     1.3.1

── Conflicts ────────────────────────────────────────── tidyverse_conflicts() ──
✖ dplyr::filter() masks stats::filter()
✖ dplyr::lag()    masks stats::lag()
ℹ Use the conflicted package (<http://conflicted.r-lib.org/>) to force all conflicts to become errors
```

``` r
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
dbExecute(conn = con, "INSTALL httpfs; LOAD httpfs;")
```

```         
[1] 0
```

``` r
PARQUET_FILE1 = "https://storage.yandexcloud.net/arrow-datasets/tm_data.pqt"
```

### Чтение датасета

``` r
sql <- "SELECT * FROM read_parquet([?])"
raw_df <- dbGetQuery(con, sql, list(PARQUET_FILE1))

raw_df %>% glimpse()
```

```         
Rows: 105,747,730
Columns: 5
$ timestamp <dbl> 1.578326e+12, 1.578326e+12, 1.578326e+12, 1.578326e+12, 1.57…
$ src       <chr> "13.43.52.51", "16.79.101.100", "18.43.118.103", "15.71.108.…
$ dst       <chr> "18.70.112.62", "12.48.65.39", "14.51.30.86", "14.50.119.33"…
$ port      <int> 40, 92, 27, 57, 115, 92, 65, 123, 79, 72, 123, 123, 22, 118,…
$ bytes     <int> 57354, 11895, 898, 7496, 20979, 8620, 46033, 1500, 979, 1036…
```

### Задание 1

#### Очистка датасета от внутренних обращений. Группировка по уникальным отправителям. Нахождение максимального объема отправленных данных

``` r
nolegitimate_flow <- raw_df %>% 
  select(src, dst, bytes) %>% 
  filter(!str_detect(dst, '1[2-4].*')) %>% 
  select(src, bytes) %>% group_by(src) %>% summarize(sum_bytes = sum(bytes)) %>% 
  filter(sum_bytes == max(sum_bytes))
nolegitimate_flow |> collect()
```

```         
# A tibble: 1 × 2
  src           sum_bytes
  <chr>             <dbl>
1 13.37.84.125 5765792351
```

### Задание 2

``` r
filter_df <- raw_df %>%
      select(timestamp, src, dst, bytes) %>%
      mutate(external_flow = (str_detect(src, '1[2-4].*') & !str_detect(dst, '1[2-4].*')),time = hour(as_datetime(timestamp/1000))) %>%
      filter(external_flow == TRUE, time >= 0 & time <= 24) %>% group_by(time) %>%
      summarise(flow_time = n()) %>% arrange(desc(flow_time))

filter_df <- filter_df %>% filter(flow_time >= mean(flow_time))

filter_df |> collect()
```

```         
# A tibble: 8 × 2
   time flow_time
  <int>     <int>
1    18   3305646
2    23   3305086
3    16   3304767
4    22   3304743
5    19   3303518
6    21   3303328
7    17   3301627
8    20   3300709
```

#### После фильтра по времени, определил максимально отправленный объем данных и отправителя

``` r
answer_df <- raw_df %>% mutate(time = hour(as_datetime(timestamp/1000))) %>% 
  filter(!str_detect(src, "^13.37.84.125")) %>% 
  filter(str_detect(src, '1[2-4].*'))  %>% filter(!str_detect(dst, '1[2-4].*'))  %>%
  filter(time >= 1 & time <= 15) %>% 
  group_by(src) %>% summarise("sum" = sum(bytes)) %>%
  select(src,sum)

answer_df <- answer_df %>% arrange(desc(sum)) %>% head(1)

answer_df |> collect()
```

```         
# A tibble: 1 × 2
  src               sum
  <chr>           <int>
1 12.55.77.96 191826796
```

### Задание 3

#### Поиск необходимого порта

``` r
filter_df_3 <- raw_df %>% filter(!str_detect(src, "^13.37.84.125")) %>% filter(!str_detect(src, "^12.55.77.96")) %>% 
  filter(str_detect(src, '1[2-4].*'))  %>%
  filter(!str_detect(dst, '1[2-4].*'))  %>% select(src, bytes, port) 


full_df <- filter_df_3 %>%  group_by(port) %>% summarise("mean_bytes"=mean(bytes), "max_bytes"=max(bytes), "sum_bytes" = sum(bytes)) %>% 
  mutate("diff_max_mean"= max_bytes-mean_bytes) %>% arrange(desc(diff_max_mean)) %>% head(1)

full_df |> collect()
```

```         
# A tibble: 1 × 5
   port mean_bytes max_bytes   sum_bytes diff_max_mean
  <int>      <dbl>     <int>       <dbl>         <dbl>
1    37     35109.    209402 23686678923       174293.
```

#### Поиск максимального объема пераданных данных через найденный ранее порт 37 и отправителя

``` r
answer_df_3 <- filter_df_3  %>% filter(port==37) %>% group_by(src) %>% 
  summarise("mean_bytes"=mean(bytes)) %>% arrange(desc(mean_bytes)) %>% select(src) %>% head(1)
answer_df_3 |> collect()
```

```         
# A tibble: 1 × 1
  src         
  <chr>       
1 14.31.107.42
```

### Выводы

Ознакомился с СУБД DuckDB для обработки и анализ больших данных совместно с языком программирования R
