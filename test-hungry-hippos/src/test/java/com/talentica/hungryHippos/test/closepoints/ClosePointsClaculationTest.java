/*******************************************************************************
 * Copyright 2017 Talentica Software Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.talentica.hungryHippos.test.closepoints;

import org.junit.Test;

public class ClosePointsClaculationTest {

  @Test
  public void distanceTest(){
    double lat1 = 41.499914;
    double lon1 = -81.559426;
    double [] lat = {39.5814970000,41.411081,41.503615,41.399161,41.612942,39.3949270000,41.358474,
        41.499914,41.497801,41.429366,41.4030790000,39.8652120000,41.406260,39.828102,41.436824,
        41.413609,41.501037,41.599278,41.6129420000,41.441101,41.590464,41.421269,30.269994,
        41.425733,39.8233000000,33.625227,30.2895220000,41.3977200000,41.3914330000,41.5219810000,
        39.3046400000,39.2916260000,39.2112790000,41.443597,41.368468};
    double [] lon = {-86.1194920000,-81.745861,-81.563514,-81.573238,-81.507036,-84.5221190000,
        -81.328283,-81.559426,-81.560740,-81.827343,-81.6865050000,-86.3828040000,-81.804359,
        -86.160743,-81.703514,-81.548886,-81.552022,-81.510502,-81.5070360000,-81.554556,-81.526002,
        -81.559162,-81.747816,-81.621100,-86.0392260000,-86.676728,-81.6414280000,-81.5032580000,
        -81.5256980000,-81.5334320000,-84.5712630000,-84.5095860000,-84.5318549000,-81.544191,
        -81.493378};
    for(int i = 0; i < lat.length; i++){
      ClosePointsWork.distance(lat1, lon1, lat[i], lon[i]);
    }
  }
}
