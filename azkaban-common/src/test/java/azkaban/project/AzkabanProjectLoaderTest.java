/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package azkaban.project;

import static azkaban.Constants.ConfigurationKeys.PROJECT_TEMP_DIR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidationStatus;
import azkaban.storage.StorageManager;
import azkaban.utils.Props;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class AzkabanProjectLoaderTest {

  private static final String DIRECTORY_FLOW_REPORT_KEY = "Directory Flow";
  private static final String BASIC_FLOW_YAML_DIR = "basicflowyamltest";
  private static final String BASIC_FLOW_FILE = "basic_flow.flow";
  private static final String PROJECT_ZIP = "Archive.zip";

  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private final int ID = 107;
  private final int VERSION = 10;
  private final Project project = new Project(this.ID, "project1");

  private AzkabanProjectLoader azkabanProjectLoader;
  private StorageManager storageManager;
  private ProjectLoader projectLoader;

  @Before
  public void setUp() throws Exception {
    final Props props = new Props();
    props.put(PROJECT_TEMP_DIR, this.TEMP_DIR.getRoot().getAbsolutePath());

    this.storageManager = mock(StorageManager.class);
    this.projectLoader = mock(ProjectLoader.class);

    this.azkabanProjectLoader = new AzkabanProjectLoader(props, this.projectLoader,
        this.storageManager, new FlowLoaderFactory(props));
  }


  private void checkValidationReport(final Map<String, ValidationReport> validationReportMap) {
    assertThat(validationReportMap.size()).isEqualTo(1);
    assertThat(validationReportMap.containsKey(DIRECTORY_FLOW_REPORT_KEY)).isTrue();
    assertThat(validationReportMap.get(DIRECTORY_FLOW_REPORT_KEY).getStatus()).isEqualTo
        (ValidationStatus.PASS);
  }
}
