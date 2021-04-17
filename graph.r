#! /usr/bin/env Rscript

library(data.table)
library(ggplot2)

raw_totals <- fread('ksqldb-original-total', header=FALSE)
totals <- data.frame(setNames(raw_totals, c('total')))
totals$input <- seq(1:nrow(totals))
ggplot(totals, aes(x = input, y = total)) +
  geom_jitter(shape = '.', height = 1, alpha = 1) +
  ylab('total') +
  xlab('output #') +     
  theme_bw()
ggsave('ksqldb-original-timeseries.png')

raw_totals <- fread('ksqldb-simplified-total', header=FALSE)
totals <- data.frame(setNames(raw_totals, c('total')))
totals$input <- seq(1:nrow(totals))
ggplot(totals, aes(x = input, y = total)) +
  geom_jitter(shape = '.', height = 1, alpha = 1) +
  ylab('total') +
  xlab('output #') +     
  theme_bw()
ggsave('ksqldb-simplified-timeseries.png')

raw_totals <- fread('flink-original-total', header=FALSE)
totals <- data.frame(setNames(raw_totals, c('total')))
totals$input <- seq(1:nrow(totals))
ggplot(totals, aes(x = input, y = total)) +
  geom_jitter(shape = '.', height = 1, alpha = 0.01) +
  ylab('total') +
  xlab('output #') +     
  theme_bw()
ggsave('flink-original-timeseries.png')

raw_totals <- fread('flink-simplified-total', header=FALSE)
totals <- data.frame(setNames(raw_totals, c('total')))
totals$input <- seq(1:nrow(totals))
ggplot(totals, aes(x = input, y = total)) +
  geom_jitter(shape = '.', height = 1, alpha = 0.01) +
  ylab('total') +
  xlab('output #') +     
  theme_bw()
ggsave('flink-simplified-timeseries.png')
