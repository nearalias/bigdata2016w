# Assignment 1

## Question 1

For Pairs implementation:
I used 2 MapReduce jobs. Since PMI calculation requires the total number of lines and how many times each word occurrs, the first job is to calculate the number of lines that each word occurred in as well as the total number of lines. The input record here is the original input files in (LongWritable, Text) format. The intermediate and output key-value format is (Text, IntWritable) since all we need here is the number of lines each word occurs. Note that total number of lines is calculated by treating "*" as "total". The 2nd job is where I calculated the number of lines each "pair" of words occurred in. Input format is still the original input file (LongWritable, Text). The intermediate key-value format is (PairOfStrings, IntWritable), and finally the output format is (PairOfStrings, FloatWritable).

For Stripes implementation:
Similar to above, I also used 2 MapReduce jobs here. The first job is exactly the same in that it calculates the total number of lines and the number of lines that each word occurs in. The 2nd job also takes the same original input, but the intermediate key-value format is (Text, HMapStIW) to store the hashmap "stripe" for each word, and the output format is (Text, HMapStFW), where each map entry is a float indicating the PMI value.

## Quesiton 2

Running on linux.student.cs.uwaterloo.ca.

PairsPMI: First Job Finished in 6.105 seconds
PairsPMI: Second Job Finished in 46.75 seconds
StripesPMI: First Job Finished in 6.117 seconds
StripesPMI: Second Job Finished in 16.758 seconds

## Question 3

Running on linux.student.cs.uwaterloo.ca, without combiners.

PairsPMI: First Job Finished in 9.114 seconds
PairsPMI: Second Job Finished in 58.791 seconds
StripesPMI: First Job Finished in 9.088 seconds
StripesPMI: Second Job Finished in 19.732 seconds

## Question 4

38599 pairs.

## Question 5

(maine, anjou) 3.6331422
I think these 2 words had the highest PMI because perhaps neither of them appears that frequently, and so the denominator in PMI calculation is lower in comparison with other pairs. At the same time, since these 2 words don't appear very often, it makes their appearances together that much more significant.

## Question 6

(tears, shed) 2.1117902
(tears, salt) 2.052812
(tears, eyes) 1.165167

(death, father's) 1.120252
(death, die)  0.7541594
(death, life) 0.73813456

## Question 7

(waterloo, kitchener) 2.6149974
(waterloo, napoleon)  1.9084398
(waterloo, napoleonic)  1.786619

(toronto, marlboros)  2.3539965
(toronto, spadina)  2.3126037
(toronto, leafs)  2.3108907
