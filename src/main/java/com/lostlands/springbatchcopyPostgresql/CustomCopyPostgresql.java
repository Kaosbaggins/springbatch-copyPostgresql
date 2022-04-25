package com.lostlands.springbatchcopyPostgresql;

import org.postgresql.copy.CopyManager;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


import com.mercadona.framework.fwkbatch3.wrappers.relational.bbdd.tasklet.ExecuteSQLTasklet;

import java.io.FileReader;
import java.sql.Connection;

import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgConnection;

import java.sql.Statement;

import javax.sql.DataSource;


@NoArgsConstructor
@Slf4j
public class CustomCopyPostgresql extends ExecuteSQLTasklet  implements Tasklet {

	  @Getter
	  @Setter
	  private Object value;

	  @Getter
	  @Setter
	  private String pathFile;

      @Autowired
      DataSource mainDataSource;
      
      
      protected String dataDirectory;
      protected String metadataFile;
      protected static Connection conn;
  	protected static Statement st;


	  /**
	   * {@inheritDoc}
	   */
	  @Override
	  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
	      throws Exception {

			try {
			    Assert.hasText(pathFile, "El path del fichero de entrada es nulo o vacío");
				log.info("Importing data from {}",pathFile);

				CopyManager cpManager = new CopyManager((BaseConnection) (PgConnection ) mainDataSource.getConnection().unwrap(PgConnection.class));

		        String copyCommand = "COPY public.pedidos_internos (pin_codpin, pin_codtda, pin_codalm, pin_codgpr, pin_codgpi, pin_fecfor, pin_fectie, pin_fecrea, pin_estado, pin_volreg, pin_volnre, pin_linreg, pin_linnre, pin_tipped, pin_volped, pin_linped, pin_inires, pin_finres, pin_volenv, pin_linenv, pin_volrec, pin_linrec, pin_volser, pin_linser, pin_pesped, pin_palped, pin_ordcab, pin_pesser, pin_palser, pin_estkal, pin_estenv, pin_inipre, pin_indenv, pin_sisori, pin_anadid) FROM STDIN DELIMITER '|' CSV ";
				log.info("copy command executed: {}",copyCommand);

		        // import csv into postgres db
				Long n = cpManager.copyIn(copyCommand, new FileReader(pathFile));
		        log.info("{} data rows imported",n);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
			

	    return RepeatStatus.FINISHED;

	  }

}