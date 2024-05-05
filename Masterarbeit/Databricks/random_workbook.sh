%sh
ls -l /Workspace/Users/tobiasfuehles@aol.com/tpcds-install.sh
/Workspace/Users/tobiasfuehles@aol.com/tpcds-install.sh

%sh
sudo apt-get update
rm -r 

%sh
# Change directory to the parent directory of the folder
cd /dbfs

# Adjust permissions recursively for the 'libra' folder
chmod -R 777 ttt

%sh
# Attempt to list files in /dbfs/mnt (optional)
ls /tmp

%sh
# Attempt to list files in /dbfs/mnt (optional)
ls /mnt

# Change directory to /dbfs/mnt
cd /mnt

# Attempt to clone the repository
git clone https://github.com/databricks/tpcds-kit.git

# Check if the cloning was successful
if [ $? -eq 0 ]; then
    # Change directory to the tools directory
    cd tpcds-kit/tools

    # Run make with appropriate OS parameter
    make OS=LINUX
else
    echo "Failed to clone the repository. Please check the network connection"
fi


#sudo apt-get --assume-yes install gcc make flex bison byacc git
#sudo apt-get install dos2unix

%sh
# Attempt to list files in /dbfs/mnt (optional)
ls /Workspace/Users/tobiasfuehles@aol.com

# Change directory to /dbfs/mnt
cd /Workspace/Users/tobiasfuehles@aol.com

# Attempt to clone the repository
git clone https://github.com/databricks/tpcds-kit.git

# Check if the cloning was successful
if [ $? -eq 0 ]; then
    # Change directory to the tools directory
    cd tpcds-kit/tools

    # Run make with appropriate OS parameter
    make OS=LINUX
else
    echo "Failed to clone the repository. Please check the network connection"
fi


#sudo apt-get --assume-yes install gcc make flex bison byacc git
#sudo apt-get install dos2unix

%sh
dos2unix /Workspace/Users/tobiasfuehles@aol.com/tpcds-install.sh

%sh
/Workspace/Users/tobiasfuehles@aol.com/tpcds-install.sh