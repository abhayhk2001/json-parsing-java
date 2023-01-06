sudo apt update
sudo apt upgrade -y
sudo apt install default-jre default-jdk git -y
git clone https://github.com/abhayhk2001/json-parsing-java
cd json-parsing-java/

javac @./args.argfile Parse_TableSaw.java
java @./args.argfile Parse_TableSaw