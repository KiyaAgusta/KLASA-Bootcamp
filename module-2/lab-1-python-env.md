# Managing Python Environments with Conda

A virtual environment is a tool that helps to keep dependencies required by different projects separate by creating isolated python virtual environments for them. This is one of the most important tools that most of the Python developers use


With conda, you can create, export, list, remove, and update environments that have different versions of Python and/or packages installed in them. Switching or moving between environments is called activating the environment. You can also share an environment file.

## Let's start on your bash shell (terminal) 
## 1. Create a new environment called 'klasa-env' that contains Python 3.7

`conda create -n klasa-env python=3.7`

Conda checks what additional packages ("dependencies") will be needed, and asks if you want to proceed.

`Proceed ([y]/n)? y`

Type y and press Enter

## 2. Using your environment 

To activate environment (conda 4.6 and later versions)

`conda activate klasa-env`

To list all available environments

`conda info --envs` <br>
The active environment is the one with an asterisk (*).

To deactivate environment

`conda deactivate klasa-env`

## 3. Set up python environment as Jupyter Kernel

Activate the python environment you want to set up jupyter kernel into

`conda activate klasa-env`

Install all dependencies needed to use jupyter

`pip install ipykernel`

Install the kernel for this environment and specify the display name

`python -m ipykernel install --user --name klasa-env --display-name 'KLASA Env'` 

Refresh your jupyter web page and select your new jupyter kernel!
