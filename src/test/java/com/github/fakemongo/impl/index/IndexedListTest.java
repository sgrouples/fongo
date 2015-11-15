package com.github.fakemongo.impl.index;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class IndexedListTest {
    private Random random;

    @Before
    public void setUp() throws Exception {
        random = new Random();
    }

    @Test
    public void testAdd() throws Exception {
        int firstElement = random.nextInt();
        int secondElement = random.nextInt();
        int thirdElement = random.nextInt();

        List<Integer> innerList = new ArrayList<Integer>();
        innerList.add(firstElement);
        IndexedList<Integer> list = new IndexedList<Integer>(innerList);
        list.add(secondElement);
        list.add(thirdElement);

        assertEquals(firstElement, list.getElements().get(0).intValue());
        assertEquals(secondElement, list.getElements().get(1).intValue());
        assertEquals(thirdElement, list.getElements().get(2).intValue());
    }

    @Test
    public void testSizeWithDuplicates() throws Exception {
        int firstElement = random.nextInt();
        int secondElement = random.nextInt();
        int thirdElement = random.nextInt();

        List<Integer> innerList = new ArrayList<Integer>();
        innerList.add(firstElement);
        IndexedList<Integer> list = new IndexedList<Integer>(innerList);
        list.add(secondElement);
        list.add(thirdElement);
        list.add(firstElement);
        list.add(secondElement);
        list.add(thirdElement);

        assertEquals(firstElement, list.getElements().get(0).intValue());
        assertEquals(secondElement, list.getElements().get(1).intValue());
        assertEquals(thirdElement, list.getElements().get(2).intValue());

        assertEquals(6, list.size());
    }

    @Test
    public void testContains() throws Exception {
        random = new Random();

        int firstElement = 42;
        int secondElement = random.nextInt();

        List<Integer> innerList = new ArrayList<Integer>();
        innerList.add(firstElement);
        IndexedList<Integer> list = new IndexedList<Integer>(innerList);
        list.add(secondElement);

        assertTrue(list.contains(firstElement));
        assertTrue(list.contains(secondElement));
    }

    @Test
    public void testContainsWithoutSecondElement() throws Exception {
        int firstElement = 12;
        int secondElement = 13;

        List<Integer> innerList = new ArrayList<Integer>();
        innerList.add(firstElement);
        IndexedList<Integer> list = new IndexedList<Integer>(innerList);

        assertTrue(list.contains(firstElement));
        assertFalse(list.contains(secondElement));
    }

    @Test
    public void testContainsSame() throws Exception {
        int firstElement = 12;
        int secondElement = 12;

        List<Integer> innerList = new ArrayList<Integer>();
        innerList.add(firstElement);
        IndexedList<Integer> list = new IndexedList<Integer>(innerList);

        assertTrue(list.contains(firstElement));
        assertTrue(list.contains(secondElement));
    }

    @Test
    public void testRemove() throws Exception {
        random = new Random();

        int firstElement = random.nextInt();
        int secondElement = random.nextInt();

        List<Integer> innerList = new ArrayList<Integer>();
        innerList.add(firstElement);
        IndexedList<Integer> list = new IndexedList<Integer>(innerList);
        list.add(secondElement);

        assertTrue(list.contains(firstElement));
        assertTrue(list.contains(secondElement));

        list.remove(firstElement);

        assertFalse(list.contains(firstElement));
        assertTrue(list.contains(secondElement));

        list.remove(secondElement);

        assertFalse(list.contains(firstElement));
        assertFalse(list.contains(secondElement));

        assertEquals(0, list.size());
    }

    @Test
    public void testRemoveSame() throws Exception {
        random = new Random();

        int firstElement = 42;
        int secondElement = 42;

        List<Integer> innerList = new ArrayList<Integer>();
        innerList.add(firstElement);
        IndexedList<Integer> list = new IndexedList<Integer>(innerList);
        list.add(secondElement);

        assertTrue(list.contains(firstElement));
        assertTrue(list.contains(secondElement));

        list.remove(firstElement);

        assertTrue(list.contains(firstElement));
        assertTrue(list.contains(secondElement));

        list.remove(secondElement);

        assertFalse(list.contains(firstElement));
        assertFalse(list.contains(secondElement));
    }
}