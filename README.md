# suredis
A speedy & simplistic library at runtime for an incredibly straightforward Redis interface.

The bar is to make all supported Redis operations take under 1ms to complete on localhost. Which to my knowledge, is achieved for now. 

An exception may be when you pass *a lot* of arguments to a command.

# Coverage
suredis strongly supports the following:
  - Commands inside of the list, hash, key, set, generic, and string types. 

suredis partially supports the following: 
  - Advanced commands. Mostly through the `manual` method as of now.

suredis has plans to implement or enhance the following (in no particular order):
  - Async.
  - Sorted Sets.
  - Transactions.
  - Advanced commands. 
  - Python dictionaries. 
  - Pipeline interfaces.
  - More object orientation.

Implementation is driven by what superficially will be used most practically in a general purpose case.

# Build Requirements
All versions you use must be compatible with the versions listed here:
  - Rust, 1.39
    - Cargo, 1.44.1
      - "redis" crate, 0.16.0
      - "pyo3" crate, 0.11.1
  - Python, (CPython) 3.5
    - "maturin" package, 0.8.2
    
# Building
suredis is not yet released, but, you can still build it using:
  - `git clone https://github.com/wellinthatcase/suredis`
  - `pip install maturin`
  - `RUSTFLAGS="--emit=asm"` (Optional, slower compilation time, but more LLVM optimization.)
  - `cargo build --release && maturin develop --release`

Make sure you have a virtual environment activated to use maturin in the build process.
Also, suredis is 1.92 GB (2,067,718,144 bytes) after compilation (if you decide to build source). Make sure you have enough disk space. 

# Contribution 
There's a few things I'd like to ask for help with: 
  - Adding/Improving Redis operations. 
    - I would rather the completion of all the current types over implementing advanced commands right now. 
      - There's some commands missing with major Redis types, after these are all done then it's time for advanced commands. 

  - Documentation. 
    - There's some inconsistancies and small errors in the current source documentation.
    - Or, if you'd like a propose an overhaul. Perhaps a new style, go ahead.

  - Code quality. 
    - I'm really interested in seeing how I can cut down on cloning strings. (.to_string & that sort.)
    - I'm also curious if I can cut down on allocations. 
    - How can dynamic types be returned? PyClass doesn't support generics. 

If you plan on contributing, try and make your contribution stay with the flow of the other code. 
  - For example, no documentation that is blatantly much different from the current documentation style.

# Information
  This is a learner's project.

  I encourage you to give the package a shot, report any problems, and give as many suggestions as possible.

  If you use & like the project, drop a star. It's nice to know I've been able to help someone out. 

  If there is any way at all the code or documentation can be improved, please let me know via issues, or a PR.
