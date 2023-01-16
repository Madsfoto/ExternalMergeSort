Changes from the original code: (Completed)
- Upgrade to .net 4.8
- Adding comandline input and output filename selection

(In progress)
- Leave only unique (.distinct) lines in the sorted smaller files


I have a specific requirement to find the unique lines, hence the third change

Original readme: 
----
----

External Merge Sort
----

This is the source code to a blog post I wrote here:

http://splinter.com.au/sorting-enormous-files-using-a-c-external-mer

Basically it sorts files that are too big to fit in memory, using an external merge sort.