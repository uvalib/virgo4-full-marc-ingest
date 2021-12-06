package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/applicationautoscaling"
	"github.com/aws/aws-sdk-go/service/ecs"
	"log"
)

var ecsService *ecs.ECS
var aasService *applicationautoscaling.ApplicationAutoScaling

// set up our ECS management objects
func init() {

	sess, err := session.NewSession()
	if err == nil {
		ecsService = ecs.New(sess)
		aasService = applicationautoscaling.New(sess)
	}
}

func ensureServicesExist(clusterName string, services []string) error {
	return nil
}

func stopManagedServices(clusterName string, managedServices []string) error {
	for _, s := range managedServices {
		err := serviceStop(clusterName, s)
		if err != nil {
			return err
		}
	}

	return nil
}

func startManagedServices(clusterName string, managedServices []string) error {
	for _, s := range managedServices {
		err := serviceStart(clusterName, s)
		if err != nil {
			return err
		}
	}

	return nil
}

// taken from https://docs.aws.amazon.com/sdk-for-go/api/service/ecs/#ECS.UpdateService

func serviceStop(clusterName string, serviceName string) error {

	log.Printf("INFO: stopping %s/%s", clusterName, serviceName)

	// suspend the autoscale rule application
	suspend := true
	suspend_state := &applicationautoscaling.SuspendedState{
		DynamicScalingInSuspended:  &suspend,
		DynamicScalingOutSuspended: &suspend,
		ScheduledScalingSuspended:  &suspend,
	}

	aasParams := &applicationautoscaling.RegisterScalableTargetInput{
		ResourceId:        aws.String(fmt.Sprintf("service/%s/%s", clusterName, serviceName)),
		ScalableDimension: aws.String("ecs:service:DesiredCount"),
		ServiceNamespace:  aws.String("ecs"),
		SuspendedState:    suspend_state,
	}

	// update autoscale rules
	_, err := aasService.RegisterScalableTarget(aasParams)
	if err != nil {
		log.Printf("WARNING: autoscale adjust failed, probably no autoscale rules")
	}

	// desired count to 0
	ecsParams := &ecs.UpdateServiceInput{
		DesiredCount: aws.Int64(0),
		Service:      aws.String(serviceName),
		Cluster:      aws.String(clusterName),
	}

	// update the service attributes
	_, err = ecsService.UpdateService(ecsParams)
	if err != nil {
		return err
	}

	return nil
}

func serviceStart(clusterName string, serviceName string) error {

	log.Printf("INFO: starting %s/%s", clusterName, serviceName)

	// un-suspend the autoscale rule application
	suspend := false
	suspend_state := &applicationautoscaling.SuspendedState{
		DynamicScalingInSuspended:  &suspend,
		DynamicScalingOutSuspended: &suspend,
		ScheduledScalingSuspended:  &suspend,
	}

	aasParams := &applicationautoscaling.RegisterScalableTargetInput{
		ResourceId:        aws.String(fmt.Sprintf("service/%s/%s", clusterName, serviceName)),
		ScalableDimension: aws.String("ecs:service:DesiredCount"),
		ServiceNamespace:  aws.String("ecs"),
		SuspendedState:    suspend_state,
	}

	// update autoscale rules
	_, err := aasService.RegisterScalableTarget(aasParams)
	if err != nil {
		log.Printf("WARNING: autoscale adjust failed, probably no autoscale rules")
	}

	// desired count to 1
	ecsParams := &ecs.UpdateServiceInput{
		DesiredCount: aws.Int64(1),
		Service:      aws.String(serviceName),
		Cluster:      aws.String(clusterName),
	}

	// update the service attributes
	_, err = ecsService.UpdateService(ecsParams)
	if err != nil {
		return err
	}

	return nil
}

//
// end of file
//
