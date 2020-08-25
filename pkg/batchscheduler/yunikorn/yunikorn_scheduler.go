/*
Copyright 2019 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package yunikorn

import (
	"fmt"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"github.com/apache/incubator-yunikorn-k8shim/pkg/apis/yunikorn.apache.org/v1alpha1"
	yunikornclient "github.com/apache/incubator-yunikorn-k8shim/pkg/client/clientset/versioned"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	schedulerinterface "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/batchscheduler/interface"
)

const (
	YunikornApplicationName = "applications.yunikorn.apache.org"
)

type YunikornBatchScheduler struct {
	extensionClient apiextensionsclient.Interface
	yunikornClient  yunikornclient.Interface
}

func GetPluginName() string {
	return "yunikorn"
}

func (y *YunikornBatchScheduler) Name() string {
	return GetPluginName()
}

func (y *YunikornBatchScheduler) ShouldSchedule(app *v1beta2.SparkApplication) bool {
	//NOTE: There is no additional requirement for yunikorn scheduler
	return true
}

func (y *YunikornBatchScheduler) DoBatchSchedulingOnSubmission(app *v1beta2.SparkApplication) error {
	if app.Spec.Executor.Annotations == nil {
		app.Spec.Executor.Annotations = make(map[string]string)
	}

	if app.Spec.Driver.Annotations == nil {
		app.Spec.Driver.Annotations = make(map[string]string)
	}

	if app.Spec.Mode == v1beta2.ClientMode {
		return y.syncYunikornAppInClientMode(app)
	} else if app.Spec.Mode == v1beta2.ClusterMode {
		return y.syncYunikornAppInClusterMode(app)
	}
	return nil
}

func (y *YunikornBatchScheduler) syncYunikornAppInClientMode(app *v1beta2.SparkApplication) error {
	if _, ok := app.Spec.Executor.Annotations["YunikoenApplication/Name"]; !ok {
		if err := y.syncYunikornApplication(app, 1); err == nil {
			app.Spec.Executor.Annotations["YunikoenApplication/Name"] = y.getYunikornAppName(app)
		} else {
			return err
		}
	}
	return nil
}

func (y *YunikornBatchScheduler) syncYunikornAppInClusterMode(app *v1beta2.SparkApplication) error {
	if _, ok := app.Spec.Executor.Annotations["YunikoenApplication/Name"]; !ok {
		if err := y.syncYunikornApplication(app, 1); err == nil {
			app.Spec.Executor.Annotations["YunikoenApplication/Name"] = y.getYunikornAppName(app)
			app.Spec.Driver.Annotations["YunikoenApplication/Name"] = y.getYunikornAppName(app)
		} else {
			return err
		}
	}
	return nil
}

func (y *YunikornBatchScheduler) getYunikornAppName(app *v1beta2.SparkApplication) string {
	return fmt.Sprintf("spark-%s-ykapp", app.Name)
}

func (y *YunikornBatchScheduler) syncYunikornApplication(app *v1beta2.SparkApplication, size int32) error {
	var err error
	ykAppName := y.getYunikornAppName(app)
	var ykapp *v1alpha1.Application
	if ykapp, err = y.yunikornClient.ApacheV1alpha1().Applications(app.Namespace).Get(ykAppName, metav1.GetOptions{}); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		ykApplication := v1alpha1.Application{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: app.Namespace,
				Name:      ykAppName,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(app, v1beta2.SchemeGroupVersion.WithKind("SparkApplication")),
				},
			},
			Spec: v1alpha1.ApplicationSpec{
				MinMember:         size,
				Queue:             "root.default",
				MaxPendingSeconds: 10,
			},
			Status: v1alpha1.ApplicationStatus{
				AppStatus: v1alpha1.NewApplicationState,
			},
		}
		if app.Spec.BatchSchedulerOptions != nil {
			if app.Spec.BatchSchedulerOptions.Queue != nil {
				ykApplication.Spec.Queue = *app.Spec.BatchSchedulerOptions.Queue
			}
		}
		_, err = y.yunikornClient.ApacheV1alpha1().Applications(app.Namespace).Create(&ykApplication)
	} else {
		if ykapp.Spec.MinMember != size {
			ykapp.Spec.MinMember = size
			_, err = y.yunikornClient.ApacheV1alpha1().Applications(app.Namespace).Update(ykapp)
		}
	}
	if err != nil {
		return fmt.Errorf("failed to sync YunikornApplication with error: %s. Abandon schedule pods via yunikorn", err)
	}

	return nil
}

func New(config *rest.Config) (schedulerinterface.BatchScheduler, error) {
	ykClient, err := yunikornclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize yunikorn client with error %v", err)
	}
	extClient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize k8s extension client with error %v", err)
	}

	if _, err := extClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(
		YunikornApplicationName, metav1.GetOptions{}); err != nil {
			return nil, fmt.Errorf("yunikornApplication CRD is request to exists in current cluster error: %s", err)
	}
	return &YunikornBatchScheduler{
		extensionClient: extClient,
		yunikornClient:  ykClient,
	}, nil
}
