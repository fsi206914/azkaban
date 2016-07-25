/*
 * Copyright 2012 LinkedIn Corp.
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
 */

$.namespace('azkaban');

var schedulePanelView;
azkaban.SchedulePanelView = Backbone.View.extend({
  events: {
    "click #schedule-button": "scheduleFlow"
  },

  initialize: function(settings) {
    $("#timepicker").datetimepicker({pickDate: false});
    $("#datepicker").datetimepicker({pickTime: false});
  },

  render: function() {
  },

  showSchedulePanel: function() {
    $('#schedule-modal').modal();
  },

  hideSchedulePanel: function() {
    $('#schedule-modal').modal("hide");
  },

  scheduleFlow: function() {
    var timeVal = $('#timepicker').val();
    var timezoneVal = $('#timezone').val();

    var dateVal = $('#datepicker').val();

    var is_recurringVal = $('#is_recurring').val();
    var periodVal = $('#period').val();
    var periodUnits = $('#period_units').val();

    var scheduleURL = contextURL + "/schedule"
    var scheduleData = flowExecuteDialogView.getExecutionOptionData();

    console.log("Creating schedule for " + projectName + "." +
        scheduleData.flow);

    var scheduleTime = moment(timeVal, 'h/mm A').format('h,mm,A,') + timezoneVal;
    console.log(scheduleTime);

    var scheduleDate = $('#datepicker').val();
    var is_recurring = document.getElementById('inlineRadio2').checked
        ? 'on' : 'off';
    var period = $('#period').val() + $('#period_units').val();

    scheduleData.ajax = "scheduleFlow";
    scheduleData.projectName = projectName;
    scheduleData.scheduleTime = scheduleTime;
    scheduleData.scheduleDate = scheduleDate;
    scheduleData.is_recurring = is_recurring;

    if ( $('#simpleTab').hasClass('active') ) {
      scheduleData.period = period;
    }
    else{
      scheduleData.cronExpression = "0 " + $('#cron-output').val();
    }

    var successHandler = function(data) {
      if (data.error) {
        schedulePanelView.hideSchedulePanel();
        messageDialogView.show("Error Scheduling Flow", data.error);
      }
      else {
        schedulePanelView.hideSchedulePanel();
        messageDialogView.show("Flow Scheduled", data.message, function() {
          window.location.href = scheduleURL;
        });
      }
    };

    $.post(scheduleURL, scheduleData, successHandler, "json");
  }
});

$(function() {
  schedulePanelView =  new azkaban.SchedulePanelView({
    el: $('#schedule-modal')
  });
  var checkRadio = ".nave-bar-inline";
  $(checkRadio).hide();

  updateOutput();
  $("#clearCron").click(function () {
    $('#cron-output').val("* * * * ?");
    resetLabelColor();
    $("#minute_input").val("*");
    $("#hour_input").val("*");
    $("#dom_input").val("*");
    $("#month_input").val("*");
    $("#dow_input").val("?");
    $(cron_translate_id).text("")
    $(cron_translate_warning_id).text("")

    while ($("#instructions tbody tr:last").index() >= 4) {
      $("#instructions tbody tr:last").remove();
    }
  });

  $("#inlineRadio2").click(function () {
    $(checkRadio).show();
  });

  $("#inlineRadio1").click(function () {
    $(checkRadio).hide();
  });

  $("#minute_input").click(function () {
    while ($("#instructions tbody tr:last").index() >= 4) {
      $("#instructions tbody tr:last").remove();
    }
    resetLabelColor();
    $("#min_label").css("color", "red");
    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last th').html("0-59");
    $('#instructions tbody tr:last td').html("allowed values");
  });

  $("#hour_input").click(function () {
    while ($("#instructions tbody tr:last").index() >= 4) {
      $("#instructions tbody tr:last").remove();
    }
    resetLabelColor();
    $("#hour_label").css("color", "red");
    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last th').html("0-23");
    $('#instructions tbody tr:last td').html("allowed values");
  });

  $("#dom_input").click(function () {
    while ($("#instructions tbody tr:last").index() >= 4) {
      $("#instructions tbody tr:last").remove();
    }
    resetLabelColor();
    $("#dom_label").css("color", "red");
    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last th').html("1-31");
    $('#instructions tbody tr:last td').html("allowed values");

    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last').find('td').css({'class': 'danger'});
    $('#instructions tbody tr:last th').html("?");
    $('#instructions tbody tr:last td').html("Blank");
  });

  $("#month_input").click(function () {
    while ($("#instructions tbody tr:last").index() >= 4) {
      $("#instructions tbody tr:last").remove();
    }
    resetLabelColor();
    $("#mon_label").css("color", "red");
    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last th').html("1-12");
    $('#instructions tbody tr:last td').html("allowed values");
  });

  $("#dow_input").click(function () {
    while ($("#instructions tbody tr:last").index() >= 4) {
      $("#instructions tbody tr:last").remove();
    }
    resetLabelColor();
    $("#dow_label").css("color", "red");

    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last th').html("1-7");
    $('#instructions tbody tr:last td').html("SUN MON TUE WED THU FRI SAT");

    $('#instructions tbody').append($("#instructions tbody tr:first").clone());
    $('#instructions tbody tr:last th').html("?");
    $('#instructions tbody tr:last td').html("Blank");
  });
});

function resetLabelColor(){
  $("#min_label").css("color", "black");
  $("#hour_label").css("color", "black");
  $("#dom_label").css("color", "black");
  $("#mon_label").css("color", "black");
  $("#dow_label").css("color", "black");
}

var cron_minutes_id = "#minute_input";
var cron_hours_id   = "#hour_input";
var cron_dom_id     = "#dom_input";
var cron_months_id  = "#month_input";
var cron_dow_id     = "#dow_input";
var cron_output_id  = "#cron-output";
var cron_translate_id  = "#cronTranslate";
var cron_translate_warning_id  = "#translationWarning";

// Cron use 0-6 as Sun--Sat, but Quartz use 1-7. Therefore, a translation is necessary.
function transformFromCronToQuartz(str){
  var res = str.split(" ");
  res[res.length -1] = res[res.length -1].replace(/[0-7]/g, function upperToHyphenLower(match) {
    return (parseInt(match)+6)%7;
  });
  return res.join(" ");
}

function updateOutput() {
  $(cron_output_id).val( $(cron_minutes_id).val() + " " +  $(cron_hours_id).val() + " " +
      $(cron_dom_id).val() + " " + $(cron_months_id).val() + " " + $(cron_dow_id).val()
  );
  updateExpression();
}

function updateExpression() {
  $(cron_translate_id).text( "\"" + prettyCron.toString( transformFromCronToQuartz($(cron_output_id).val())) + "\"");
  $(cron_translate_warning_id).html( " <small>***The execution plan translation has limitations. Please check the <a href=\"http://www.quartz-scheduler.org/documentation/quartz-2.x/tutorials/crontrigger.html\">Quartz-Cron syntax</a> when in doubt.***</small>" );
}
