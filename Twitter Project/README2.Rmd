# 1st Place Winner of Unicode Coding Challenge 2021 - Twitter Bot Project

This directory contains a twitter bot project submitted to **Unicode Research**,
an online teaching and research organization which provides classes and competitions
for students to engage in.

The goal of this project is to create a computer program that can classify injury
reports from twitter data in Baseball. The best model found during the 8-week course
and competition was a pre-trained XLNet Neural Network.

Among all applicants, this project won 1st place for its direct impact/inspiration
on industry and soundness of research.

```{r}
knitr::kable(head(read.csv('results.csv')), format = "simple")
```