README:

-------------------------------------------------------------------------------

This README file will explain how to start instances of the Sharded Distributed Data Store with (partial) Chord.

The source code for this application can be viewed in the "src" folder.

All of the test files are featured in the "test" folder.

If you would like to start and instance of the application, (or mulitple instances of the application), I have provided multiple "runserver" files for you to use.  They are featured in the "test" folder.  I have provided 8 of these files, but you may simply copy the contents of these files to make more.

If you would like to run multiple instances of this application on your local machine in order to test it out or simply play around with it, simply first download the repository, (obviously), then run the command "./test/build.sh" .
This command will build the Docker container with the application installed.

Now all you need to do to spin up an instance of the application is invoke the Docker run command.  These are contained in the aforementioned "runserver" files featured in the "test" folder.

An example of the Docker run command is provided below:

docker run -p 13801:13800 --net=kv_subnet --ip=10.10.0.4 --name="node1" -e ADDRESS="10.10.0.4:13800" -e VIEW="10.10.0.4:13800,10.10.0.5:13800,10.10.0.6:13800,10.10.0.7:13800,10.10.0.8:13800" kvs

Each of the strings following a capitalized variable name (e.g., "ADDRESS") is
an environment variable that will be inserted into the container upon startup.  This is how the local instance knows its own addres, as well as the addresses of the other nodes in the system upon startup.  For the system to function as expected, the other nodes featured in the run command's "VIEW" variable must also be initialized before requests of the system can be made.

If you wish to kill all servers or containers, simply open a new terminal or command prompt and simply run the command "./test/stoprm.sh" .  This command will stop and remove all running containers on your machine.  

Remember, you may alter or play around with the "runserver" commands as you see fit.

Have fun!