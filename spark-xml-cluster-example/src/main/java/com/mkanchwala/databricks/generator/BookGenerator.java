package com.mkanchwala.databricks.generator;

import java.util.ArrayList;
import java.util.List;

import com.mkanchwala.databricks.model.Book;

public class BookGenerator {
	
	public static List<Book> generate(Integer generateIndex){
		
		List<Book> books = new ArrayList<Book>();
		
		for (int index = 0; index < generateIndex; index++) {
			books.add(new Book("Book" + index, " Some Test description " + index));
		}
		
		return books;
	}

}
