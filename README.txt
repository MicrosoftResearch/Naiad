Installation steps
------------------

Naiad is distributed as a Visual Studio 2012 solution, comprising 
the following projects:

* "Naiad", which contains the core Naiad system and support libraries,
  and builds Naiad.dll. If you create your own project that uses
  Naiad, you must add a reference to Naiad.dll.

* "Lindi", a LINQ-like library for data-parallel iterative computation.

* "DifferentialDataflow" a LINQ-like library supporting incremental 
  execution of iterative computation.

* "Examples", which contains several example Naiad applications and
  builds Examples.exe.

These projects can be built and executed using any of the following
implementations of the .NET framework:

* Microsoft .NET Framework 4.5

To build Naiad, follow the steps below that correspond to your setup.

* Building using Visual Studio

  1. Open the Naiad.sln file in this directory using Visual Studio.
  2. On the menu bar, click Build > Build Solution, or press F6.

* Building using MSBuild (on Windows, without Visual Studio)

  1. Ensure that MSBuild is installed on your local machine. You can
     get MSBuild by installing the Microsoft .NET Framwork, version
     4.5. Typically, the file MSBuild.exe can be found in the path:

     C:\Windows\Microsoft.NET\Framework64\v4.0.30319\MSBuild.exe

     However, this path may differ, depending on your local
     configuration.

  2. Open a command prompt, and change to the directory containing the
     Naiad.sln file.

  3. At the command prompt, run MSBuild.exe.

Getting started with the example programs
-----------------------------------------

As part of this source distribution, the Examples project demonstrates 
how to write simple applications using Naiad. After building the Naiad 
solution, you will find the executable Examples.exe in the 
Examples\bin\Debug directory.

Examples.exe uses command-line arguments to select an example, and it 
is recommended that you run it from a command prompt or terminal. For 
example, to run the word count example, follow these steps:

1. Open a command prompt or terminal, and change to the
   Examples\bin\Debug directory.

2. At the command prompt, type "Examples.exe" to see the usage and 
   available examples.

3. At the command prompt, type "Examples.exe wordcount" to get started 
   with the wordcount example.

Known issues with Naiad
-----------------------

This release of Naiad is an alpha release. The amount of testing the code 
has been subjected to is limited, mostly to programs we have written. The 
email address naiadquestions@microsoft.com goes to each of the project 
members, and is a great place to send questions and comments.

There are several specific issues we know about now, and want to point 
out to avoid headaches for everyone involved: 

1.	In distributed mode, we strongly recommend that each process call 
	OnNext the same numbers of times before calling OnCompleted. 
	Several aspects of the graph manager logic consult the epoch numbers 
	associated with local inputs. Ideally, all process call OnNext the 
	same number of times; passing no parameters to OnNext causes the 
	epoch to advance with no input supplied, and is a great way to do this.

2.	Naiad currently requires custom datatypes to be structs, rather 
	than classes. This is not too hard to fix, but does require us 
	to swing through the code and ensure that every class has an 
	appropriate default constructor, which all structs have. Structs 
	also stress the GC a lot less, and are a lot easier to auto-serialize. 
	Related to this, if you want to use distributed Naiad, the 
	auto-generated Serialization/Deserialization code requires all of 
	your fields to be public and mutable (ie: not readonly). This is 
	because the generated code attempts to set these values on a newly 
	minted struct, and can’t do this without these settings. More 
	details on writing custom serialization code in upcoming blog posts.

As Naiad is a research prototype, there are many parts of it that are 
likely to change. We’ve tried hard to put interfaces in place that will 
remain relatively stable, but there are certainly aspects of the 
interfaces we expect will change. Information about programming examples, 
the current state of the code, known problems, and upcoming fixes and 
features should be available at the project web page:  

    http://research.microsoft.com/Naiad/,

or at the related blog:

    http://bigdataatsvc.wordpress.com/


Contacting us
-------------

If you have questions or comments, please feel welcome to email us at

    naiadquestions@microsoft.com

We'd love to hear your thoughts on the project. Thanks!

The Naiad team.