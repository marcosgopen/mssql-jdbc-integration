package io.test.tm;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerResource;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;


public class SimpleTestCase {
    static boolean failure = false;

    public static void main(String[] args) throws SQLException, InterruptedException, XAException, IOException {
        if (System.getProperty("URL") == null || System.getProperty("username") == null || System.getProperty("password") == null) {
            throw new RuntimeException("Could not find required configuration");
        }
        SQLServerXADataSource ds = new SQLServerXADataSource();
        ds.setURL(System.getProperty("URL"));
        ds.setConnectRetryCount(0);

        XAConnection xaConnection = null;
        boolean gotRMFAIL = false;
        try {
            xaConnection = ds.getXAConnection(System.getProperty("username"), System.getProperty("password"));
            XAResource xar = xaConnection.getXAResource();

            // Make sure there are no Xids from a previous "tomtest"
            Xid[] xids = xar.recover(XAResource.TMSTARTRSCAN);
            for (Xid xid : xids) {
                String gtrid = new String(xid.getGlobalTransactionId());
                if (gtrid.equals("SimpleTestCase")) {
                    if (Boolean.parseBoolean(System.getProperty("WARNcommitRecoveredXidsWithMatchingGtrid"))) {
                        xar.commit(xid, false);
                    }
                }
            }
            xar.recover(XAResource.TMENDRSCAN);

            Xid xid = new Xid() {
                @Override
                public int getFormatId() {
                    return 1;
                }

                @Override
                public byte[] getGlobalTransactionId() {
                    return "SimpleTestCase".getBytes();
                }

                @Override
                public byte[] getBranchQualifier() {
                    return new byte[0];
                }
            };
            xar.start(xid, XAResource.TMNOFLAGS);
            xar.end(xid, XAResource.TMSUCCESS);
            xar.prepare(xid);


            System.out.println("Toggle the network access to the server");
            System.in.read();

            try {
                xar.commit(xid, false);
            } catch (XAException xae) {
                if (xae.errorCode == XAException.XAER_RMERR) {
                    System.err.println("the error code should not be XAER_RMERR");
                    xae.printStackTrace();
                    failure = true;
                } else if (xae.errorCode == XAException.XAER_RMFAIL ){
                    gotRMFAIL = true;
                }
            }
        } finally {
            try {
                if (xaConnection != null) {
                    xaConnection.close();
                }
            } finally {
                try {
                    // Tidy up
                    xaConnection = ds.getXAConnection(System.getProperty("username"), System.getProperty("password"));
                    XAResource xar = xaConnection.getXAResource();
                    Xid[] xids = xar.recover(XAResource.TMSTARTRSCAN);
                    boolean foundAndCommitedXid = false;
                    for (Xid xid : xids) {
                        String gtrid = new String(xid.getGlobalTransactionId());
                        if (gtrid.equals("SimpleTestCase")) {
                            xar.commit(xid, false);
                            foundAndCommitedXid = true;
                        }
                    }
                    if (failure) {
                        if (foundAndCommitedXid) {
                            System.err.println("Given MsSQL reported that it an RMERR, it is unexpected that it was eventually able to find and commit the Xid so that should be considered a failure too");
                            failure = true;
                        }
                    } else if (gotRMFAIL){
                        if (!foundAndCommitedXid) {
                            System.err.println("Recieving RMFAIL we would expect to be able to recover the branch");
                            failure = true;
                        }
                    }
                    xar.recover(XAResource.TMENDRSCAN);
                } finally {
                    if (xaConnection != null) {
                        xaConnection.close();
                    }
                }
            }
        }

        System.out.println("Did the test fail?: " + failure);
        System.exit(failure ? -1 : 0);
    }
}
