
package main;

import com.amazonaws.services.simpleemail.model.Message;
import dbtransformationphase.TransformatinPhase;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.JTextArea;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Ashrin Rose Jose
 */
public class HomeGUI extends javax.swing.JFrame {

    /**
     * Creates new form HomeGUI
     */
    public HomeGUI() {
        initComponents();
        phase1Label.setVisible(false);
        phase2Label.setVisible(false);
        phase3Label.setVisible(false);
        progressIcon.setVisible(false);

        PrintStream printStream = new PrintStream(new CustomOutputStream(consoleField));
        System.setOut(printStream);
        System.setErr(printStream);
    }

   
    @SuppressWarnings("unchecked")

    private void initComponents() {

        fileChooser = new javax.swing.JFileChooser();
        jPanel1 = new javax.swing.JPanel();
        jButton1 = new javax.swing.JButton();
        jButton2 = new javax.swing.JButton();
        jLabel3 = new javax.swing.JLabel();
        dbLocField = new javax.swing.JTextField();
        saveLocField = new javax.swing.JTextField();
        jLabel6 = new javax.swing.JLabel();
        jPanel4 = new javax.swing.JPanel();
        jLabel8 = new javax.swing.JLabel();
        hadoopFSPortField = new javax.swing.JTextField();
        hadoopFolderloc = new javax.swing.JTextField();
        jLabel9 = new javax.swing.JLabel();
        jPanel5 = new javax.swing.JPanel();
        jLabel5 = new javax.swing.JLabel();
        KField = new javax.swing.JTextField();
        jLabel4 = new javax.swing.JLabel();
        minUtilityField = new javax.swing.JTextField();
        lblMinUtil2 = new javax.swing.JLabel();
        minUtilityField2 = new javax.swing.JTextField();
        jPanel2 = new javax.swing.JPanel();
        jLabel1 = new javax.swing.JLabel();
        jLabel2 = new javax.swing.JLabel();
        jLabel7 = new javax.swing.JLabel();
        phase1Label = new javax.swing.JLabel();
        phase3Label = new javax.swing.JLabel();
        phase2Label = new javax.swing.JLabel();
        jPanel3 = new javax.swing.JPanel();
        jScrollPane1 = new javax.swing.JScrollPane();
        consoleField = new javax.swing.JTextArea();
        beginBTN = new javax.swing.JButton();
        progressIcon = new javax.swing.JLabel();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        setTitle("High Utility Miner");

        jPanel1.setBorder(javax.swing.BorderFactory.createTitledBorder("Job Settings"));

        jButton1.setText("Change");
        jButton1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton1ActionPerformed(evt);
            }
        });

        jButton2.setText("Change");
        jButton2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton2ActionPerformed(evt);
            }
        });

        jLabel3.setText("Input Database Location:");

        dbLocField.setEditable(false);
        dbLocField.setText("/home/user/ashrin/input1.txt");

        saveLocField.setEditable(false);
        saveLocField.setText("/home/user/ashrin/HUIout.txt");

        jLabel6.setText("Save Location:");

        jPanel4.setBorder(javax.swing.BorderFactory.createTitledBorder("Hadoop Job Settings"));

        jLabel8.setText("Hadoop FS Port:");

        hadoopFSPortField.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        hadoopFSPortField.setText("9000");

        hadoopFolderloc.setText("/project/");

        jLabel9.setText("HDFS Folder Location:");

        javax.swing.GroupLayout jPanel4Layout = new javax.swing.GroupLayout(jPanel4);
        jPanel4.setLayout(jPanel4Layout);
        jPanel4Layout.setHorizontalGroup(
            jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel4Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jLabel8)
                    .addComponent(hadoopFSPortField, javax.swing.GroupLayout.PREFERRED_SIZE, 212, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(hadoopFolderloc)
                    .addGroup(jPanel4Layout.createSequentialGroup()
                        .addComponent(jLabel9)
                        .addGap(0, 0, Short.MAX_VALUE)))
                .addContainerGap())
        );
        jPanel4Layout.setVerticalGroup(
            jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel4Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel8)
                    .addComponent(jLabel9))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(hadoopFSPortField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(hadoopFolderloc, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap())
        );

        jPanel5.setBorder(javax.swing.BorderFactory.createTitledBorder("Parameters"));

        jLabel5.setText("K (Iterations):");

        KField.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        KField.setText("3");

        jLabel4.setText("Mini.Utility-Counting Phase:");

        minUtilityField.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        minUtilityField.setText("30");

        lblMinUtil2.setText("Min.Utility-Mining Phase:");

        minUtilityField2.setHorizontalAlignment(javax.swing.JTextField.CENTER);
        minUtilityField2.setText("10");

        javax.swing.GroupLayout jPanel5Layout = new javax.swing.GroupLayout(jPanel5);
        jPanel5.setLayout(jPanel5Layout);
        jPanel5Layout.setHorizontalGroup(
            jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel5Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(jLabel4, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(minUtilityField))
                .addGap(18, 18, 18)
                .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(lblMinUtil2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(minUtilityField2))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 46, Short.MAX_VALUE)
                .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(KField)
                    .addComponent(jLabel5, javax.swing.GroupLayout.DEFAULT_SIZE, 100, Short.MAX_VALUE))
                .addContainerGap())
        );
        jPanel5Layout.setVerticalGroup(
            jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel5Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel4)
                    .addComponent(jLabel5, javax.swing.GroupLayout.PREFERRED_SIZE, 17, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(lblMinUtil2))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                        .addComponent(minUtilityField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addComponent(KField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addComponent(minUtilityField2, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap())
        );

        javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
        jPanel1.setLayout(jPanel1Layout);
        jPanel1Layout.setHorizontalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addGroup(jPanel1Layout.createSequentialGroup()
                        .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(jButton2, javax.swing.GroupLayout.PREFERRED_SIZE, 92, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(jPanel1Layout.createSequentialGroup()
                        .addGap(39, 39, 39)
                        .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addGroup(jPanel1Layout.createSequentialGroup()
                                .addComponent(jLabel3, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                .addGap(17, 17, 17))
                            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel1Layout.createSequentialGroup()
                                .addComponent(dbLocField, javax.swing.GroupLayout.PREFERRED_SIZE, 375, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                .addComponent(jButton1, javax.swing.GroupLayout.PREFERRED_SIZE, 92, javax.swing.GroupLayout.PREFERRED_SIZE))
                            .addGroup(jPanel1Layout.createSequentialGroup()
                                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                    .addComponent(jLabel6)
                                    .addComponent(saveLocField, javax.swing.GroupLayout.PREFERRED_SIZE, 376, javax.swing.GroupLayout.PREFERRED_SIZE))
                                .addGap(0, 0, Short.MAX_VALUE))
                            .addComponent(jPanel4, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                            .addComponent(jPanel5, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))))
                .addGap(37, 37, 37))
        );
        jPanel1Layout.setVerticalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addGap(24, 24, 24)
                .addComponent(jLabel3)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jButton1, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(dbLocField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addGap(18, 18, 18)
                .addComponent(jPanel5, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addGap(10, 10, 10)
                .addComponent(jPanel4, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addGap(18, 18, 18)
                .addComponent(jLabel6)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(saveLocField, javax.swing.GroupLayout.PREFERRED_SIZE, 28, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jButton2))
                .addContainerGap(22, Short.MAX_VALUE))
        );

        jLabel1.setText("Counting Phase");

        jLabel2.setText("Transformation Phase  ");

        jLabel7.setText("Mining Phase");

        phase1Label.setIcon(new javax.swing.ImageIcon(getClass().getResource("/main/check.png"))); // NOI18N

        phase3Label.setIcon(new javax.swing.ImageIcon(getClass().getResource("/main/check.png"))); // NOI18N

        phase2Label.setIcon(new javax.swing.ImageIcon(getClass().getResource("/main/check.png"))); // NOI18N

        javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(jPanel2);
        jPanel2.setLayout(jPanel2Layout);
        jPanel2Layout.setHorizontalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel2Layout.createSequentialGroup()
                .addGap(20, 20, 20)
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                    .addComponent(jLabel7, javax.swing.GroupLayout.PREFERRED_SIZE, 166, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel2, javax.swing.GroupLayout.PREFERRED_SIZE, 166, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel1, javax.swing.GroupLayout.PREFERRED_SIZE, 166, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(phase1Label, javax.swing.GroupLayout.PREFERRED_SIZE, 49, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(phase3Label, javax.swing.GroupLayout.PREFERRED_SIZE, 49, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(phase2Label, javax.swing.GroupLayout.PREFERRED_SIZE, 49, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        jPanel2Layout.setVerticalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel2Layout.createSequentialGroup()
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jLabel1, javax.swing.GroupLayout.PREFERRED_SIZE, 23, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(phase1Label, javax.swing.GroupLayout.PREFERRED_SIZE, 26, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jLabel2, javax.swing.GroupLayout.PREFERRED_SIZE, 23, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(phase2Label, javax.swing.GroupLayout.PREFERRED_SIZE, 26, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jLabel7, javax.swing.GroupLayout.PREFERRED_SIZE, 23, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(phase3Label, javax.swing.GroupLayout.PREFERRED_SIZE, 26, javax.swing.GroupLayout.PREFERRED_SIZE)))
        );

        jPanel3.setLayout(new java.awt.BorderLayout());

        jScrollPane1.setBorder(javax.swing.BorderFactory.createTitledBorder("Console"));

        consoleField.setBackground(java.awt.Color.darkGray);
        consoleField.setColumns(20);
        consoleField.setForeground(new java.awt.Color(255, 246, 119));
        consoleField.setRows(5);
        jScrollPane1.setViewportView(consoleField);

        jPanel3.add(jScrollPane1, java.awt.BorderLayout.CENTER);

        beginBTN.setFont(new java.awt.Font("Ubuntu", 1, 16)); // NOI18N
        beginBTN.setText("BEGIN UTILITY MINING");
        beginBTN.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                beginBTNActionPerformed(evt);
            }
        });

        progressIcon.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        progressIcon.setIcon(new javax.swing.ImageIcon(getClass().getResource("/main/loading_animation.gif"))); // NOI18N

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jPanel3, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(jPanel1, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addGroup(layout.createSequentialGroup()
                        .addGap(21, 21, 21)
                        .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                            .addComponent(beginBTN, javax.swing.GroupLayout.DEFAULT_SIZE, 226, Short.MAX_VALUE)
                            .addComponent(progressIcon, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                        .addGap(18, 18, 18)
                        .addComponent(jPanel2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))
                .addContainerGap())
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addComponent(jPanel1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addGroup(layout.createSequentialGroup()
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(jPanel2, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(layout.createSequentialGroup()
                        .addGap(34, 34, 34)
                        .addComponent(beginBTN)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(progressIcon, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jPanel3, javax.swing.GroupLayout.DEFAULT_SIZE, 173, Short.MAX_VALUE)
                .addContainerGap())
        );

        pack();
        setLocationRelativeTo(null);
    }// </editor-fold>//GEN-END:initComponents

    private void beginBTNActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_beginBTNActionPerformed
        // TODO add your handling code here:
        phase1Label.setVisible(false);
        phase3Label.setVisible(false);
        phase2Label.setVisible(false);
        new Thread(new Runnable() {

            @Override
            public void run() {

                try {
                    progressIcon.setVisible(true);
                    beginBTN.setEnabled(false);

                    String inputDB = dbLocField.getText();
                    String saveLoc = saveLocField.getText();
                    long minUtility = Long.parseLong(minUtilityField.getText());
                    long minUtility2=Long.parseLong(minUtilityField2.getText());
                    int K = Integer.parseInt(KField.getText());
                    String hdfsPort = hadoopFSPortField.getText().trim();
                    String hdfsFolder = hadoopFolderloc.getText().trim();

                    String tempLoc1 = new File(inputDB).getParent() + "/phase1db.txt";
                    String tempLoc2 = new File(inputDB).getParent() + "/phase2db.txt";

                    String[] jobSet = {"Phase1", inputDB, tempLoc1, hdfsPort, hdfsFolder};

                    System.out.println("\n MapReduce Counting Phase Has Started");

                    /**
                     * Phase 1.
                     */
                    ToolRunner.run(new Configuration(), new JobRunner(), jobSet);

                    phase1Label.setVisible(true);

                    String[] jobSet2 = {inputDB, tempLoc1, tempLoc2};
                    long time = TransformatinPhase.execute(jobSet2, minUtility);
                    phase2Label.setVisible(true);

                    /**
                     * Phase 3.
                     */
                    String[] jobSet3 = {"Phase3", tempLoc2, saveLoc,
                        String.valueOf(minUtility2), String.valueOf(K), hdfsPort, hdfsFolder};

                    ToolRunner.run(new Configuration(), new JobRunner(), jobSet3);
                    phase3Label.setVisible(true);

                    BufferedReader br = new BufferedReader(new FileReader(jobSet3[2]));

                    HashSet HUIs = new HashSet();

                    String line;
                    while ((line = br.readLine()) != null) {
                        String HUI = line.split("\\s+")[0];
                        HUIs.add(HUI);
                    }
                    System.out.println("HUI = " + HUIs);

                    time += JobRunner.totalTime;
                    System.out.println("Total time = " + time + "ms");

                    beginBTN.setEnabled(true);
                    progressIcon.setVisible(false);

                } catch (Exception ex) {
                    Logger.getLogger(Home.class.getName()).log(Level.SEVERE, null, ex);
                    beginBTN.setEnabled(true);
                    progressIcon.setVisible(false);
                    JOptionPane.showMessageDialog(rootPane, ex.getMessage(), "Error!", JOptionPane.ERROR_MESSAGE);
                }

            }
        }).start();


    }//GEN-LAST:event_beginBTNActionPerformed

    private void jButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed
        // TODO add your handling code here:
        int retVal = fileChooser.showOpenDialog(jButton1);
        if (retVal == JFileChooser.APPROVE_OPTION) {
            File selectedFile = fileChooser.getSelectedFile();
            dbLocField.setText(selectedFile.getAbsolutePath());
        }
    }//GEN-LAST:event_jButton1ActionPerformed

    private void jButton2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton2ActionPerformed

        int retVal = fileChooser.showSaveDialog(jButton2);
        if (retVal == JFileChooser.APPROVE_OPTION) {
            File selectedFile = fileChooser.getSelectedFile();
            saveLocField.setText(selectedFile.getAbsolutePath());
        }
        // TODO add your handling code here:
    }//GEN-LAST:event_jButton2ActionPerformed

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(HomeGUI.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(HomeGUI.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(HomeGUI.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(HomeGUI.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new HomeGUI().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JTextField KField;
    private javax.swing.JButton beginBTN;
    private javax.swing.JTextArea consoleField;
    private javax.swing.JTextField dbLocField;
    private javax.swing.JFileChooser fileChooser;
    private javax.swing.JTextField hadoopFSPortField;
    private javax.swing.JTextField hadoopFolderloc;
    private javax.swing.JButton jButton1;
    private javax.swing.JButton jButton2;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JLabel jLabel4;
    private javax.swing.JLabel jLabel5;
    private javax.swing.JLabel jLabel6;
    private javax.swing.JLabel jLabel7;
    private javax.swing.JLabel jLabel8;
    private javax.swing.JLabel jLabel9;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JPanel jPanel3;
    private javax.swing.JPanel jPanel4;
    private javax.swing.JPanel jPanel5;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JLabel lblMinUtil2;
    private javax.swing.JTextField minUtilityField;
    private javax.swing.JTextField minUtilityField2;
    private javax.swing.JLabel phase1Label;
    private javax.swing.JLabel phase2Label;
    private javax.swing.JLabel phase3Label;
    private javax.swing.JLabel progressIcon;
    private javax.swing.JTextField saveLocField;
    // End of variables declaration//GEN-END:variables
}

class CustomOutputStream extends OutputStream {

    private JTextArea textArea;

    public CustomOutputStream(JTextArea textArea) {
        this.textArea = textArea;
    }

    @Override
    public void write(int b) throws IOException {
        // redirects data to the text area
        textArea.append(String.valueOf((char) b));
        // scrolls the text area to the end of data
        textArea.setCaretPosition(textArea.getDocument().getLength());
    }
}
