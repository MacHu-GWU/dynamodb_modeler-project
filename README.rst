Welcome to "Dynamodb Modeler" Project
==============================================================================

.. image:: https://user-images.githubusercontent.com/6800411/212983034-b0d8f048-228e-4be6-b591-1e39a70d64ec.png

This project is a DynamoDB modeling tool. It is a playground for developer to design a DynamoDB data model for business use case and implements business logic, user interaction and query patterns.


How to Use
------------------------------------------------------------------------------
Please read this 📔 `Example.ipynb <./Example.ipynb>`_ Jupyter Notebook to learn how. And you can duplicate the  `Example.ipynb <./Example.ipynb>`_ to start your own project.

Common Relationship Patterns:

- `One to Many - Method 1, Multiple Tables <./One-To-Many-Relationship-in-DynamoDB-Method-1.ipynb>`_
- `Many to Many Relationship In DynamoDB - Method 1 Adjacency List <./Many-To-Many-Relationship-in-DynamoDB-Method-1.ipynb>`_


Install Dependencies
------------------------------------------------------------------------------
.. code-block:: console

    virtualenv -p python3.10 .venv

.. code-block:: console

    source .venv/bin/activate

.. code-block:: console

    pip install -r requirements.txt

.. code-block:: console

    .venv/bin/jupyter-lab
