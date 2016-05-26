## About
Arietis is a simple maven multi-module project which can do insertion from a db's table to another's. It is written in java 6 and can operate on oracle, mysql and mssql. 

## Motivation
I built it after a failure to migrate a table from a db to another because of row limitation of client tool which i used. So, it is one of my first java applications. But recently i moved the codes to make it a maven project.

## Features
  - Various RMDBSs support
  - Lightweight
  - Quick bulk insertion with prepared statement batching
  - Query independent

## Considered Features to Add
  - Update operations support
  - Using an excel file as a source or a target

## Installation

Run the following code

```git clone https://github.com/kahramani/arietis.git```

Run ```mvn install``` on your path of the project

Then finally run ```java Operator.java``` on dbOperator/src/main/java

