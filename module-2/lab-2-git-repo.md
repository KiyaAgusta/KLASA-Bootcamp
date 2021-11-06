# Github Repository
Working with Github repositories, forking, cloning, merging, requesting pulls, solving (easy) conflicts.

## Let's start on your bash shell (terminal) 

Create a directory folder for your repository

`mkdir gitklasa_tanyani`

Get in the directory to start working

```cd gitklasa_tanyani```

Initialize a LOCAL git repository so files in this directory can be tracked on your own machine

```
git init
git status
``` 

Create a text file! 

`echo "Hello World!" >> myfirstfile.txt`

Verify that it was created

```
ls -l
git status
```

Let's try committing it to github? 

```
git commit myfirstfile.txt -m 'first commit'
git push
```

You should have gotten an error because you have not told this repo where in the cloud to look for its remote version

On your browser, go to https://github.com and create a repository

Follow the direction in the image above in the same directory of your terminal

```
git remote add origin https://github.com/tanyani/gittest_tanyani.git
git push -u origin master
```

Now make local changes to your file

```
echo "hello again" >> myfirstfile.txt
git status
```

You should be able to view the file you changed that needs to be committed. Add the changes you wish to commit, commit with a message, then push to your repository.

```
git add myfirstfile.txt
git commit -m 'updating my text file'
git push
```

Now go online to see your github repository and make changes the file online through the github site.


