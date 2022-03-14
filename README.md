## LSM-Tree Database

## What is an LSM-Tree?

The first time I came across LSM-Tree was when I read the book "Data Intensive Application Design", in which the authors
derived a database based on LSM-Tree from the simplest DB implementation model, and this is what I wanted to implement
in this repository - a database based LSM-Tree. In addition to learning about LSM-Tree, I also wanted to learn more
about my Go language skills, so I will be using Go throughout this project to translate my ideas into code.

So what exactly is an LSM Tree? I would recommend reading the book "Data Intensive Application Design", which is a great
introduction to the LSM-Tree and its step-by-step reasoning, but I will also write down my own humble knowledge of the
LSM-Tree here.

The full name of the LSM is Log Structured-Merge Tree,

## How to implement a database based on LSM-Tree ?

### How to use it

An embedded database that can be used by external projects via package references.
