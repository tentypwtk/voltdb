/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.messaging.impl;

import java.io.IOException;
import java.net.*;
import java.nio.channels.Selector;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import junit.framework.*;
import org.voltdb.VoltDB;
import org.voltdb.messaging.*;
import org.voltdb.network.VoltNetwork;
import org.voltdb.utils.DBBPool;

public class TestMessaging extends TestCase {

    public static class MsgTest extends VoltMessage {

        public static final byte MSG_TEST_ID = 127;

        static {
            VoltMessage.externals.put(MSG_TEST_ID, MsgTest.class);
        }

        static byte[] globalValue;
        byte[] m_localValue;
        int m_length;

        static void initWithSize(int size) {
            Random r = new Random();
            globalValue = new byte[size];
            for (int i = 0; i < size; i++)
                globalValue[i] = (byte) r.nextInt(Byte.MAX_VALUE);
        }

        @Override
        protected void initFromBuffer() {
            m_buffer.position(HEADER_SIZE + 1); // ignore msg type
            m_length = m_buffer.limit() - m_buffer.position();
            m_localValue = new byte[m_length];
            m_buffer.get(m_localValue);
        }

        @Override
        protected void flattenToBuffer(final DBBPool pool) {
            m_buffer.position(HEADER_SIZE);
            setBufferSize(1 + m_localValue.length, pool);
            m_buffer.put(MSG_TEST_ID);
            m_buffer.put(m_localValue);
        }

        public void setValues() {
            m_localValue = new byte[globalValue.length];
            for (int i = 0; i < globalValue.length; i++)
                m_localValue[i] = globalValue[i];
        }

        public boolean verify() {
            if (globalValue.length != m_localValue.length)
                return false;
            for (int i = 0; i < globalValue.length; i++) {
                if (globalValue[i] != m_localValue[i]) {
                    System.err.printf("MsgTst.verify() failed at byte: %d\n", i);
                    return false;
                }
            }
            return true;
        }
    }

    public static class MsgTestEndpoint extends Thread {
        static final int msgCount = 1024;
        static final int hostCount = 3;
        static final int mailboxCount = 5;
        static VoltNetwork network = new VoltNetwork();

        static Lock hostMessengerLock = new ReentrantLock();
        static Messenger[] messengers = new HostMessenger[hostCount];
        static AtomicInteger sitesDone = new AtomicInteger(0);

        static AtomicInteger sentCount = new AtomicInteger(0);
        static AtomicInteger recvCount = new AtomicInteger(0);
        static Random rand = new Random();
        static AtomicInteger siteCount = new AtomicInteger(0);

        int mySiteId;

        public MsgTestEndpoint() {
            mySiteId = siteCount.getAndIncrement();
        }

        @Override
        public void run() {
            // create a site
            int hostId = mySiteId % hostCount;
            InetAddress leader = null;
            Messenger currentMessenger = null;
            Mailbox[] mbox = new Mailbox[mailboxCount];

            System.out.printf("Starting up host: %d, site: %d\n", hostId, mySiteId);

            hostMessengerLock.lock();
            {
                // get the host if possible
                currentMessenger = messengers[hostId];

                // create the host if needed
                if (currentMessenger == null) {
                    boolean isPrimary = hostId == 0;
                    if (isPrimary)
                    {
                        try {
                            System.out.printf("Host/Site %d/%d is initializing the HostMessenger class.\n", hostId, mySiteId);
                            leader = InetAddress.getLocalHost();
                        } catch (UnknownHostException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.printf("Host/Site %d/%d is creating a new HostMessenger.\n", hostId, mySiteId);
                    HostMessenger messenger = new HostMessenger(network, leader, hostCount, null);
                    currentMessenger = messenger;
                    messengers[hostId] = currentMessenger;
                }
                else
                    System.out.printf("Host/Site %d/%d found existing HostMessenger.\n", hostId, mySiteId);
            }
            hostMessengerLock.unlock();

            HostMessenger messenger = (HostMessenger) currentMessenger;
            messenger.waitForGroupJoin();
            // create the site
            messenger.createLocalSite(mySiteId);
            // create the mailboxes
            for (int i = 0; i < mailboxCount; i++) {
                mbox[i] = currentMessenger.createMailbox(mySiteId, i, null);
            }
            // claim this site is done
            sitesDone.incrementAndGet();
            System.out.printf("Host/Site %d/%d has joined and created sites.\n", hostId, mySiteId);

            // spin until all sites are done
            while (sitesDone.get() < siteCount.get()) {
            }

            System.out.printf("Host/Site %d/%d thinks all threads are ready.\n", hostId, mySiteId);

            // begin loop
            while(recvCount.get() < msgCount) {
                // figure out which message to send
                int msgIndex = sentCount.getAndIncrement();

                // send a message
                if (msgIndex < msgCount) {
                    int siteId = rand.nextInt(siteCount.get());
                    int mailboxId = rand.nextInt(mailboxCount);
                    System.out.printf("Host/Site %d/%d is sending message %d/%d to site/mailbox %d/%d.\n",
                            hostId, mySiteId, msgIndex, msgCount, siteId, mailboxId);
                    MsgTest mt = new MsgTest();
                    mt.setValues();
                    try {
                        ((HostMessenger) currentMessenger).send(siteId, mailboxId, mt);
                    } catch (MessagingException e) {
                        e.printStackTrace();
                    }
                }

                // try to recv a message
                for (int i = 0; i < mailboxCount; i++) {
                    MsgTest mt = (MsgTest) mbox[i].recv();
                    if (mt != null) {
                        int recvCountTemp = recvCount.incrementAndGet();
                        System.out.printf("Host/Site %d/%d is receiving message %d/%d.\n",
                                hostId, mySiteId, recvCountTemp, msgCount);
                        assertTrue(mt.verify());
                    }
                }
            }
            try {
                network.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /*public void testSerialization() {
        final int REPEAT = 1024 * 1024 * 5;

        try {
            FastSerializer fout;
            FastDeserializer fin;

            MsgTest mt = new MsgTest();

            long t1 = new Date().getTime();

            for (int i = 0; i < REPEAT; i++) {
                fout = new FastSerializer();
                fout.writeObject(mt);
                byte[] buf = fout.getBytes();
                fin = new FastDeserializer(buf);
                mt = (MsgTest) fin.readObject(mt.getClass());
            }

            long t2 = new Date().getTime();

            double time = (t2 - t1) / 1000.0;
            long throughput = (long)(REPEAT / time);
            System.out.printf("S/D pairs per sec: %d\n", throughput);

        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            ByteArrayOutputStream baos;
            ByteArrayInputStream bais;
            ObjectOutputStream out;
            ObjectInputStream in;

            MsgTest mt = new MsgTest();

            long t1 = new Date().getTime();

            for (int i = 0; i < REPEAT; i++) {
                baos = new ByteArrayOutputStream();
                out = new ObjectOutputStream(baos);
                out.writeObject(mt);
                byte[] buf = baos.toByteArray();
                bais = new ByteArrayInputStream(buf);
                in = new ObjectInputStream(bais);
                mt = (MsgTest) in.readObject();
            }

            long t2 = new Date().getTime();

            double time = (t2 - t1) / 1000.0;
            long throughput = (long)(REPEAT / time);
            System.out.printf("S/D pairs per sec: %d\n", throughput);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }*/

    public void testJoiner() {
        try {
            SocketJoiner joiner1 = new SocketJoiner(InetAddress.getLocalHost(), 3, null);
            SocketJoiner joiner2 = new SocketJoiner(InetAddress.getLocalHost(), 3, null);
            SocketJoiner joiner3 = new SocketJoiner(InetAddress.getLocalHost(), 3, null);

            joiner1.start();
            joiner2.start();
            joiner3.start();

            joiner1.join();
            joiner2.join();
            joiner3.join();

            assertTrue(true);
            return;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertTrue(false);
    }

    public void testSimple() throws MessagingException, UnknownHostException, InterruptedException {
        try {
            Selector.open();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        VoltNetwork network = new VoltNetwork();
        network.start();
        HostMessenger msg1 = new HostMessenger(network, InetAddress.getLocalHost(), 2, null);
        HostMessenger msg2 = new HostMessenger(network, InetAddress.getLocalHost(), 2, null);

        System.out.println("Waiting for socketjoiners...");
        msg1.waitForGroupJoin();
        System.out.println("Finished socket joiner for msg1");
        msg2.waitForGroupJoin();
        System.out.println("Finished socket joiner for msg2");

        assertTrue(msg1.getHostId() == 0);
        assertTrue(msg2.getHostId() == 1);

        int siteId1 = msg1.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 1;
        int siteId2 = msg2.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 2;

        msg1.createLocalSite(siteId1);
        msg2.createLocalSite(siteId2);

        Mailbox mb1 = msg1.createMailbox(siteId1, 1, null);
        Mailbox mb2 = msg2.createMailbox(siteId2, 1, null);

        MsgTest.initWithSize(16);
        MsgTest mt = (MsgTest) VoltMessage.createNewMessage(MsgTest.MSG_TEST_ID);
        mt.setValues();
        mb1.send(siteId2, 1, mt);
        MsgTest mt2 = null;
        while (mt2 == null) {
            mt2 = (MsgTest) mb2.recv();
        }
        assertTrue(mt2.verify());

        // Do it again
        MsgTest.initWithSize(32);
        mt = (MsgTest) VoltMessage.createNewMessage(MsgTest.MSG_TEST_ID);
        mt.setValues();
        mb1.send(siteId2, 1, mt);
        mt2 = null;
        while (mt2 == null) {
            mt2 = (MsgTest) mb2.recv();
        }
        assertTrue(mt2.verify());

        // Do it a final time with a message that should block on write.
        // spin on a complete network message here - maybe better to write
        // the above cases this way too?
        for (int i=0; i < 3; ++i) {
            System.out.println("running iteration: " + i);

            MsgTest.initWithSize(4280034);
            mt = (MsgTest) VoltMessage.createNewMessage(MsgTest.MSG_TEST_ID);
            mt.setValues();
            mb1.send(siteId2, 1, mt);
            mt2 = null;
            while (mt2 == null) {
                mt2 = (MsgTest) mb2.recv();
            }
            assertTrue(mt.verify());
        }
        network.shutdown();
    }

    public void testMultiMailbox() throws MessagingException, UnknownHostException, InterruptedException {
        VoltNetwork network = new VoltNetwork();
        network.start();
        HostMessenger msg1 = new HostMessenger(network, InetAddress.getLocalHost(), 3, null);
        HostMessenger msg2 = new HostMessenger(network, InetAddress.getLocalHost(), 3, null);
        HostMessenger msg3 = new HostMessenger(network, InetAddress.getLocalHost(), 3, null);

        System.out.println("Waiting for socketjoiners...");
        msg1.waitForGroupJoin();
        System.out.println("Finished socket joiner for msg1");
        msg2.waitForGroupJoin();
        System.out.println("Finished socket joiner for msg2");
        msg3.waitForGroupJoin();
        System.out.println("Finished socket joiner for msg3");

        assertTrue(msg1.getHostId() != msg2.getHostId() && msg2.getHostId() != msg3.getHostId());
        //assertTrue(msg2.getHostId() == 1);
        //assertTrue(msg3.getHostId() == 2);

        int siteId1 = msg1.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 1;
        int siteId5 = msg1.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 5;
        msg1.createLocalSite(siteId1);
        msg1.createLocalSite(siteId5);

        int siteId2 = msg2.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 2;
        msg2.createLocalSite(siteId2);

        int siteId3 = msg3.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 3;
        int siteId4 = msg3.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 4;
        msg3.createLocalSite(siteId3);
        msg3.createLocalSite(siteId4);

        Mailbox mb1 = msg1.createMailbox(siteId1, 1, null);
        Mailbox mb2 = msg2.createMailbox(siteId2, 1, null);
        Mailbox mb3 = msg3.createMailbox(siteId3, 1, null);
        Mailbox mb4 = msg3.createMailbox(siteId4, 1, null);
        Mailbox mb5 = msg1.createMailbox(siteId5, 1, null);

        MsgTest.initWithSize(16);
        MsgTest mt = (MsgTest) VoltMessage.createNewMessage(MsgTest.MSG_TEST_ID);
        mt.setValues();

        int msgCount = 0;

        mb1.send(new int[] {siteId2,siteId3,siteId5,siteId4}, 1, mt);
        long now = System.currentTimeMillis();
        MsgTest mt2 = null, mt3 = null, mt4 = null, mt5 = null;

        // run (for no more than 5s) until all 4 messages have arrived
        // this code is really weird, but it is more accurate than just
        // running until you get 4 messages. It actually makes sure they
        // are the right messages.
        while (msgCount < 4) {
            assertTrue((System.currentTimeMillis() - now) < 5000);

            if (mt2 == null) {
                mt2 = (MsgTest) mb2.recv();
                if (mt2 != null) {
                    assertTrue(mt2.verify());
                    msgCount++;
                }
            }
            if (mt3 == null) {
                mt3 = (MsgTest) mb3.recv();
                if (mt3 != null) {
                    assertTrue(mt3.verify());
                    msgCount++;
                }
            }
            if (mt4 == null) {
                mt4 = (MsgTest) mb4.recv();
                if (mt4 != null) {
                    assertTrue(mt4.verify());
                    msgCount++;
                }
            }
            if (mt5 == null) {
                mt5 = (MsgTest) mb5.recv();
                if (mt5 != null) {
                    assertTrue(mt5.verify());
                    msgCount++;
                }
            }
        }

        mb3.send(new int[] {siteId5}, 1, mt);

        assertEquals(mb2.getWaitingCount(), 0);
        assertEquals(mb3.getWaitingCount(), 0);
        assertEquals(mb4.getWaitingCount(), 0);

        // check that there is a single message for mb5
        // again, weird code, but I think it's right (jhugg)
        int wc = 0;
        now = System.currentTimeMillis();
        while (wc != 1) {
            assertTrue((System.currentTimeMillis() - now) < 5000);
            wc = mb5.getWaitingCount();
            if (wc == 0)
            assertTrue(wc < 2);
        }
        network.shutdown();
    }

    /*public void testForStress1() {
        final int siteCount = 3;

        MsgTest.initWithSize(64);

        MsgTestEndpoint[] endpoints = new MsgTestEndpoint[siteCount];

        for (int i = 0; i < siteCount; i++)
            endpoints[i] = new MsgTestEndpoint();

        for (int i = 0; i < siteCount; i++)
            endpoints[i].start();

        for (int i = 0; i < siteCount; i++)
            try {
                endpoints[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
    }*/
}
